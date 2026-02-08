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

## Environment Variables

- `RIAK_HOST` — Riak protobuf host address (default: `127.0.0.1` in scripts)
- `RIAK_PORT` — Riak protobuf port (default: `8087`)
- `RIAK_HTTP_PORT` — Riak HTTP API port (default: `8098`)
- `BANGFS_NAMESPACE` — Filesystem / bucket-type namespace prefix (required)
- `BANGFS_MOUNTDIR` — FUSE mount point directory (required)
- `RIAK_IMAGE` — Docker image name for Riak (default: `bangriak`)
- `RIAK_CONTAINER` — Docker container name for Riak (default: `bangtest`)

All three binaries also accept equivalent CLI flags (`-host`, `-port`, `-namespace`, `-mount`).

## Scripts

- `start-riak-docker.sh` — Starts (or reuses) a Riak Docker container and waits for it to become ready.
- `init-riak-buckets-docker.sh` — Creates the three BangFS bucket types (metadata, chunks, counters) inside a running Docker container via `docker exec`.
- `init-riak-buckets.sh` — Same as above but targets a Kubernetes Riak pod via `kubectl exec`. Takes `<namespace>` and `<k8s-namespace>` args.
- `test-riak-rest.sh` — Smoke-tests the Riak bucket types via the HTTP REST API (PUT/GET/DELETE for metadata & chunks, counter increment).
- `backend/riak-entrypoint.sh` — Docker entrypoint for the custom Riak image — sets ulimits, configures networking, and starts Riak in foreground mode.

## Usage

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

