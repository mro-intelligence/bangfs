# BangFS

P.O.C. distributed FUSE filesystem backed by Riak KV, intended to test distributed systems concepts.

## Status

In progress.

## Requirements

- Go 1.21+
- For testing: Docker
- For deployment (replicated consistent metadata store): Kubernetes on multiple nodes

## Building

```bash
make build
```

To regenerate protobuf code and build:

```bash
make all
```

Produces three binaries: `mkfs-bangfs`, `mount-fuse-bangfs`, `reformat-bangfs`.

## Usage

All commands accept `-host`, `-port`, `-namespace` flags or the environment variables `RIAK_HOST`, `RIAK_PORT`, `BANGFS_NAMESPACE`.

### Riak setup



## Design Goals

BangFS will use Riak KV as backend with two bucket types per namespace:

```text
┌─────────────┐     ┌─────────────────────────────────────┐
│  FUSE       │     │            Riak KV Backend          │
│  Client     │────▶│  ┌─────────────┬─────────────────┐  │
│  (bangfs)   │     │  │  Metadata   │     Chunks      │  │
└─────────────┘     │  │  (strongly  │  (eventually    │  │
                    │  │  consistent)│   consistent)   │  │
                    │  └─────────────┴─────────────────┘  │
                    └─────────────────────────────────────┘
```

Additionally a counter bucket generates inode numbers on the backend.
Metadata is serialized with protocol buffers (`proto/metadata.proto`).
Chunks are content-addressed by FNV-1a hash.

## Design Notes

### Pros

- **Metadata consistency** — utilities can repair chunk issues
- **Chunk performance** — no consistency overhead on chunk read/write
- **Scalability** — inherits Riak's scalability and durability

### Cons

- **Concurrent write race condition** — two clients modifying the same chunk can orphan one version
- **Metadata write rate** — proportional to write ops, slower due to strong consistency

