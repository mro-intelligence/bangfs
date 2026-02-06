.PHONY: proto build clean install-tools all

# Quick build (no proto regeneration)
build:
	go build -o mount-fuse-bangfs ./cmd/mount-fuse-bangfs
	go build -o mkfs-bangfs ./cmd/mkfs-bangfs
	go build -o reformat-bangfs ./cmd/reformat-bangfs

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
