.PHONY: proto clean install-tools all test

# Quick build (no proto regeneration)
mount-fuse-bangfs: ./cmd/mount-fuse-bangfs/*.go ./fuse/*.go
	go build -o mount-fuse-bangfs ./cmd/mount-fuse-bangfs

mkfs-bangfs: ./cmd/mkfs-bangfs/*.go ./fuse/*.go
	go build -o mkfs-bangfs ./cmd/mkfs-bangfs

reformat-bangfs: ./cmd/reformat-bangfs/*.go ./fuse/*.go
	go build -o reformat-bangfs ./cmd/reformat-bangfs

build: mount-fuse-bangfs mkfs-bangfs reformat-bangfs

# Full build with proto regeneration
all: proto build

# Generate protobuf code
proto: install-tools
	protoc --go_out=. --go_opt=paths=source_relative proto/metadata.proto

# Clean generated files and binaries
clean:
	rm -f proto/*.pb.go
	rm -f mount-fuse-bangfs mkfs-bangfs reformat-bangfs

# Install required tools (protoc-gen-go)
install-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

test:
	cd test && python3 test_bangfs.py