# BangFS

![meme](docs/meme.png)

## Motivation

Distributed filesystem on top of Riak KV, inspired by GFS. Wanted to see what happens when you use an eventually consistent KV store for both file metadata and file data and try to make it work as a real mountable filesystem.

## Design Goals

- **Homogenous** — no leader-follower, no special services. Just clients talking to a KV store. Coordination comes from the store's consistency properties.
- **Scaleable** to whatever the KV store can handle.
- **Optimistic concurrency** via CAS — no locks. Conflicting writes just fail.
- **Useable at small scale** — at least on par with spinning disk latency.

## How It Works

- File chunks go into the KV store with *eventual consistency*.
- File metadata (inodes) go in with *strong consistency*.
- Metadata writes are slower (consensus under the hood) but always consistent. Chunk writes are fast but if you read right after writing, the data might not be there yet.

## Architecture

```
POSIX I/O
    │
  FUSE (go-fuse/v2)
    │
  BangFS
    ├── dirnode.go     — directory ops
    ├── filenode.go    — file ops
    ├── fh.go          — file handle I/O
    └── idgen.go       — distributed ID gen
    │
  KVStore interface
    ├── RiakKVStore    — Riak via protobuf
    └── FileKVStore    — tmpfs (for testing)
    │
  Two bucket types
    ├── metadata       — strong consistency, vclock CAS
    └── chunks         — eventual consistency
```

- Inode number is the KV key, not stored in the metadata blob.
- Vclocks for concurrency — creates use `IfNoneMatch`, updates use `IfNotModified(vclock)`. Conflicts return EIO, no silent corruption.
- 1 MB chunks. Eventually consistent for throughput, only the metadata pointing to them is strongly consistent.
- Distributed ID gen — composite 64-bit: `timestamp_ms | seq_no | local_id`. No coordinator needed.
- Protobuf for metadata serialization.

## Lessons Learned

- **ID generation.** Started with a simple counter for inode numbers — wasn't thread-safe, only did inode IDs, would collide across clients. Had to build a real `IdGenerator` with a mutex and a composite ID scheme (timestamp + seq + client identity) that works for both inode and chunk IDs.
- **Vclocks.** Riak doesn't return the vclock after a store by default. Client was holding a stale vclock, so every subsequent CAS would fail. Fix: `WithReturnBody(true)`. Annoying to track down.
- **Block size.** Wasn't reporting `blksize` to FUSE, so the kernel was sending tiny I/O requests. Setting it to match the 1 MB chunk size made a big difference.

## Shortcomings

_Nobody would build a production system this way._

- Probably no other distributed FS uses a KV store for both metadata and data.
- Orphaned chunks if a metadata commit fails after writing chunks.
- No GC.
- Concurrent modifications to the same file/dir just fail.

## Future Scope

- **Content-addressable chunks** — key by content hash for dedup, but then you need refcounts and GC. Too much for a POC.
- **Write caching/coalescing** — right now every write is a sync round-trip per chunk. A write-back cache with async flushes on `Flush`/`Fsync` would help a lot. Biggest perf win left on the table.
- **Riak operator** — running Riak properly on k8s (ring management, anti-affinity, etc). Separate repo.

## Status

POC. Unit tests and integration tests pass. Integration tests run against single-node Riak in Docker — no multi-node consistency testing yet.

## Requirements

- Go 1.21+
- Docker (for integration tests)

## Building

Three binaries: `mkfs-bangfs`, `mount-fuse-bangfs`, `reformat-bangfs`.

## Environment Variables

- `RIAK_HOST` — protobuf host (default `127.0.0.1`)
- `RIAK_PORT` — protobuf port (default `8087`)
- `RIAK_HTTP_PORT` — HTTP port (default `8098`)
- `BANGFS_NAMESPACE` — bucket-type prefix (required)
- `BANGFS_MOUNTDIR` — FUSE mount point (required)
- `RIAK_IMAGE` — Docker image (default `bangriak`)
- `RIAK_CONTAINER` — Docker container name (default `bangtest`)

Binaries also accept CLI flags: `-host`, `-port`, `-namespace`, `-mount`.

## Setting Up Riak Buckets

Two bucket types need to exist before anything works — one for metadata, one for chunks. Named `${BANGFS_NAMESPACE}_bangfs_metadata` and `${BANGFS_NAMESPACE}_bangfs_chunks`.

Docker (single node):

```bash
./start-riak-docker.sh          # start container, wait for ready
./init-riak-buckets-docker.sh   # create + activate bucket types
```

Kubernetes:

```bash
./init-riak-buckets-k8s.sh [namespace] [k8s-namespace]
```

Scripts default to `n_val=1, w=1, r=1` for single-node. For multi-node you'd want `n_val>=3` and `consistent=true` on the metadata bucket.

## Testing

There's a Python integration test suite (`test/test_bangfs.py`) that mounts the filesystem and hits it with POSIX syscalls end-to-end — mkdir, create, read, write, truncate, append, random writes, permissions, timestamps. Runs against both the file-backed dummy and real Riak. Most useful thing in the repo for catching regressions.

```bash
make test                # unit tests, no external deps
make integration-test    # against running Riak
```
