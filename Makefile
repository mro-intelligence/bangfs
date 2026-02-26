.PHONY: clean install-tools all test build unit-test integration-test

# Default target: build all binaries
.DEFAULT_GOAL := build

# Quick build (no proto regeneration)
mount-fuse-bangfs: ./cmd/mount-fuse-bangfs/*.go ./bangfuse/*.go proto/metadata.pb.go
	go build -o mount-fuse-bangfs ./cmd/mount-fuse-bangfs

mkfs-bangfs: ./cmd/mkfs-bangfs/*.go ./bangfuse/*.go proto/metadata.pb.go
	go build -o mkfs-bangfs ./cmd/mkfs-bangfs

reformat-bangfs: ./cmd/reformat-bangfs/*.go ./bangfuse/*.go proto/metadata.pb.go
	go build -o reformat-bangfs ./cmd/reformat-bangfs

build: mount-fuse-bangfs mkfs-bangfs reformat-bangfs

# Full build with proto regeneration
all: build

# Generate protobuf code only when .proto changes
proto/metadata.pb.go: proto/metadata.proto
	protoc --go_out=. --go_opt=paths=source_relative proto/metadata.proto

# Clean generated files and binaries
clean:
	rm -f proto/*.pb.go
	rm -f mount-fuse-bangfs mkfs-bangfs reformat-bangfs

# Install required tools (protoc-gen-go) - run manually once
install-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

test: unit-test

test-all: test integration-test

# Unit tests (no external deps, uses FileKVStore)
unit-test: build
	go test -v ./bangfuse/
	cd test && python3 test_bangfs.py --dummy

# Integration tests (requires running Riak instance)
# Set RIAK_HOST, RIAK_PORT, BANGFS_NAMESPACE env vars or use defaults
integration-test: build
	go test ./bangfuse/ -v -integration
	cd test && python3 test_bangfs.py
