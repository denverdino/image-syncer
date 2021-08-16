.PHONY: cmd clean

cmd: $(wildcard ./pkg/client/*.go ./pkg/sync/*.go ./pkg/tools/*.go ./cmd/*.go ./*.go)
	go build -mod vendor -o image-syncer ./main.go

clean: 
	rm image-syncer