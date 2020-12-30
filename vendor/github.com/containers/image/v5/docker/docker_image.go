package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/containers/image/v5/docker/reference"
	"github.com/containers/image/v5/internal/image"
	"github.com/containers/image/v5/manifest"
	"github.com/containers/image/v5/types"
	"github.com/opencontainers/go-digest"
	"github.com/sirupsen/logrus"
)

// Image is a Docker-specific implementation of types.ImageCloser with a few extra methods
// which are specific to Docker.
type Image struct {
	types.ImageCloser
	src *dockerImageSource
}

// newImage returns a new Image interface type after setting up
// a client to the registry hosting the given image.
// The caller must call .Close() on the returned Image.
func newImage(ctx context.Context, sys *types.SystemContext, ref dockerReference) (types.ImageCloser, error) {
	s, err := newImageSource(ctx, sys, ref)
	if err != nil {
		return nil, err
	}
	img, err := image.FromSource(ctx, sys, s)
	if err != nil {
		return nil, err
	}
	return &Image{ImageCloser: img, src: s}, nil
}

// SourceRefFullName returns a fully expanded name for the repository this image is in.
func (i *Image) SourceRefFullName() string {
	return i.src.logicalRef.ref.Name()
}

// GetRepositoryTags list all tags available in the repository. The tag
// provided inside the ImageReference will be ignored. (This is a
// backward-compatible shim method which calls the module-level
// GetRepositoryTags)
func (i *Image) GetRepositoryTags(ctx context.Context) ([]string, error) {
	return GetRepositoryTags(ctx, i.src.c.sys, i.src.logicalRef)
}

// GetRepositoryTags list all tags available in the repository. The tag
// provided inside the ImageReference will be ignored.
func GetRepositoryTags(ctx context.Context, sys *types.SystemContext, ref types.ImageReference) ([]string, error) {
	dr, ok := ref.(dockerReference)
	if !ok {
		return nil, errors.New("ref must be a dockerReference")
	}

	registryConfig, err := loadRegistryConfiguration(sys)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf(tagsPath, reference.Path(dr.ref))
	client, err := newDockerClientFromRef(sys, dr, registryConfig, false, "pull")
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	tags := make([]string, 0)

	for {
		res, err := client.makeRequest(ctx, http.MethodGet, path, nil, nil, v2Auth, nil)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("fetching tags list: %w", registryHTTPResponseToError(res))
		}

		var tagsHolder struct {
			Tags []string
		}
		if err = json.NewDecoder(res.Body).Decode(&tagsHolder); err != nil {
			return nil, err
		}
		tags = append(tags, tagsHolder.Tags...)

		link := res.Header.Get("Link")
		if link == "" {
			break
		}

		linkURLPart, _, _ := strings.Cut(link, ";")
		linkURL, err := url.Parse(strings.Trim(linkURLPart, "<>"))
		if err != nil {
			return tags, err
		}

		// can be relative or absolute, but we only want the path (and I
		// guess we're in trouble if it forwards to a new place...)
		path = linkURL.Path
		if linkURL.RawQuery != "" {
			path += "?"
			path += linkURL.RawQuery
		}
	}
	return tags, nil
}

// GetDigest returns the image's digest
// Use this to optimize and avoid use of an ImageSource based on the returned digest;
// if you are going to use an ImageSource anyway, it’s more efficient to create it first
// and compute the digest from the value returned by GetManifest.
// NOTE: Implemented to avoid Docker Hub API limits, and mirror configuration may be
// ignored (but may be implemented in the future)
func GetDigest(ctx context.Context, sys *types.SystemContext, ref types.ImageReference) (digest.Digest, error) {
	dr, ok := ref.(dockerReference)
	if !ok {
		return "", errors.New("ref must be a dockerReference")
	}

	tagOrDigest, err := dr.tagOrDigest()
	if err != nil {
		return "", err
	}

	registryConfig, err := loadRegistryConfiguration(sys)
	if err != nil {
		return "", err
	}
	client, err := newDockerClientFromRef(sys, dr, registryConfig, false, "pull")
	if err != nil {
		return "", fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	path := fmt.Sprintf(manifestPath, reference.Path(dr.ref), tagOrDigest)
	headers := map[string][]string{
		"Accept": manifest.DefaultRequestedManifestMIMETypes,
	}

	res, err := client.makeRequest(ctx, http.MethodHead, path, headers, nil, v2Auth, nil)
	if err != nil {
		return "", err
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("reading digest %s in %s: %w", tagOrDigest, dr.ref.Name(), registryHTTPResponseToError(res))
	}

	dig, err := digest.Parse(res.Header.Get("Docker-Content-Digest"))
	if err != nil {
		return "", err
	}

	return dig, nil
}

func GetRepositoryTagsAfterDate(ctx context.Context, sys *types.SystemContext, ref types.ImageReference, date time.Time) ([]string, error) {
	dr, ok := ref.(dockerReference)
	if !ok {
		return nil, errors.New("ref must be a dockerReference")
	}

	registryConfig, err := loadRegistryConfiguration(sys)
	if err != nil {
		return nil, err
	}
	//path := fmt.Sprintf(tagsPath, reference.Path(dr.ref))
	client, err := newDockerClientFromRef(sys, dr, registryConfig, false, "pull")
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	tags := make([]string, 0)

	mapUpdatedTime := make(map[string]time.Time)

	if client.registry == "registry.hub.docker.com" {
		repositoryPath := fmt.Sprintf("/v2/repositories/%s/tags?page_size=100", reference.Path(dr.ref))
		for {
			res, err := client.makeRequest(ctx, http.MethodGet, repositoryPath, nil, nil, noAuth, nil)
			if err != nil {
				return nil, err
			}
			defer res.Body.Close()
			if err := httpResponseToError(res, "Error fetching tags list"); err != nil {
				return nil, err
			}
			buf := new(bytes.Buffer)
			buf.ReadFrom(res.Body)
			var tagsHolder struct {
				Next     string `json:"next,omitempty"`
				Previous string `json:"previous,omitempty"`
				Results  []struct {
					Name        string `json:"name,omitempty"`
					LastUpdated string `json:"last_updated,omitempty"`
				} `json:"results,omitempty"`
			}
			if err = json.NewDecoder(buf).Decode(&tagsHolder); err != nil {
				return nil, err
			}

			// Process manifest

			for _, manifest := range tagsHolder.Results {
				lastUpdated := manifest.LastUpdated
				tag := manifest.Name
				if len(lastUpdated) > 0 {
					t, err := time.Parse(time.RFC3339, lastUpdated)
					if err != nil {
						return nil, err
					}
					logrus.Debugf("tag %s updated time %s\n", tag, t.Format(time.RFC3339))
					mapUpdatedTime[tag] = t
				}
				tags = append(tags, tag)
			}

			link := tagsHolder.Next
			if link == "" {
				break
			}

			repositoryPath = link[len("https://registry.hub.docker.com"):]
		}
	} else if client.registry == "quay.io" {
		page := 1
		for {
			repositoryPath := fmt.Sprintf("/api/v1/repository/%s/tag/?onlyActiveTags=true&limit=100&page=%d", reference.Path(dr.ref), page)
			logrus.Debugf("GET %s", repositoryPath)
			res, err := client.makeRequest(ctx, http.MethodGet, repositoryPath, nil, nil, noAuth, nil)
			if err != nil {
				return nil, err
			}
			defer res.Body.Close()
			if err := httpResponseToError(res, "Error fetching tags list"); err != nil {
				return nil, err
			}
			buf := new(bytes.Buffer)
			buf.ReadFrom(res.Body)
			var tagsHolder struct {
				HasAdditional bool `json:"has_additional,omitempty"`
				Page          int  `json:"page,omitempty"`
				Tags          []struct {
					Name         string `json:"name,omitempty"`
					LastModified string `json:"last_modified,omitempty"`
					Expiration   string `json:"expiration,omitempty"`
				} `json:"tags,omitempty"`
			}
			if err = json.NewDecoder(buf).Decode(&tagsHolder); err != nil {
				return nil, err
			}

			// Process manifest
			for _, manifest := range tagsHolder.Tags {
				tag := manifest.Name
				LastModified := manifest.LastModified
				if len(manifest.Expiration) > 0 {
					//Ignore expired tag
					continue
				}
				if len(LastModified) > 0 {
					t, err := time.Parse(time.RFC1123Z, LastModified)
					if err != nil {
						return nil, err
					}
					logrus.Debugf("tag %s updated time %s\n", tag, t.Format(time.RFC3339))
					mapUpdatedTime[tag] = t
				}
				tags = append(tags, tag)
			}
			if tagsHolder.HasAdditional {
				page = tagsHolder.Page + 1
			} else {
				break
			}
		}
	} else {
		path := fmt.Sprintf(tagsPath, reference.Path(dr.ref))
		for {
			res, err := client.makeRequest(ctx, http.MethodGet, path, nil, nil, v2Auth, nil)
			if err != nil {
				return nil, err
			}
			defer res.Body.Close()
			if err := httpResponseToError(res, "Error fetching tags list"); err != nil {
				return nil, err
			}

			buf := new(bytes.Buffer)
			buf.ReadFrom(res.Body)
			var tagsHolder struct {
				Tags     []string
				Manifest map[string]struct {
					Tag            []string
					TimeCreatedMs  string
					TimeUploadedMs string
				}
			}
			if err = json.NewDecoder(buf).Decode(&tagsHolder); err != nil {
				return nil, err
			}

			// Process manifest

			for _, manifest := range tagsHolder.Manifest {
				timeUploadedMs := manifest.TimeUploadedMs
				if timeUploadedMs != "" {
					i, err := strconv.ParseInt(timeUploadedMs, 10, 64)
					if err != nil {
						logrus.Warnf("failed to parse timeUploadedMs %s", timeUploadedMs)
					} else {
						updatedTime := time.Unix(0, i*int64(time.Millisecond))
						for _, tag := range manifest.Tag {
							mapUpdatedTime[tag] = updatedTime
							logrus.Debugf("tag %s updated time %s\n", tag, updatedTime.Format(time.RFC3339))
						}
					}
				}
			}

			tags = append(tags, tagsHolder.Tags...)

			link := res.Header.Get("Link")
			if link == "" {
				break
			}

			linkURLStr := strings.Trim(strings.Split(link, ";")[0], "<>")
			linkURL, err := url.Parse(linkURLStr)
			if err != nil {
				return tags, err
			}

			// can be relative or absolute, but we only want the path (and I
			// guess we're in trouble if it forwards to a new place...)
			path = linkURL.Path
			if linkURL.RawQuery != "" {
				path += "?"
				path += linkURL.RawQuery
			}
		}
	}

	tagsResult := make([]string, 0)

	for _, tag := range tags {
		updatedTime, ok := mapUpdatedTime[tag]
		if !ok || updatedTime.After(date) {
			tagsResult = append(tagsResult, tag)
		} else {
			logrus.Debugf("Ignore tag %s", tag)
		}
	}

	return tagsResult, nil
}
