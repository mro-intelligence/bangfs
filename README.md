# BangFS

Experimental FUSE distributed filesystem with minimal components:

- A key value store for metadata.
- A key value store for file data chunks
- A client program

## Architecture

```
  ┌────────────────────────────┐
  │  library calls (user)      │  (ls, cp, touch, mkdir)
  └────┬───────────────────────┘
       │
       │ FUSE   (userspace ↔ kernel ↔ userspace)
       │
  ┌────┴─────┐
  │  BangFS  │  mount-fs-bangfs, mkfs-bangfs
  └────┬─────┘
       │ KVStore interface
       ├──────────────────────┐
  ┌────┴──────┐         ┌─────┴──────┐
  │  Metadata │         │  Chunks    │
  └───────────┘         └────────────┘
```

### Key value store

#### Metadata

Contains almost all fields in a vfs inode, ie what you see when you `stat file`.
- **Metadata** requirement (inodes, directory entries): stored in a **strongly consistent** bucket. Reads always see previous writes. 
- Metadata is keyed by inode number. The metadata values are single protobuf messages.
- Metadata representing file inodes contains a list of chunk keys for each fixed-size chunk of data in the file.

#### File chunks
- Files are broken up into as same-size chunks (except for the last chunk in a file, which may be shorter). 
- Chunks will be stored in an **eventually consistent** bucket: a read immediately after a write may not reflect the latest data.

### Concurrency

#### *Metadata*: 

Concurrent access to metadata via vector clocks (vclock). Metadata updates use CAS with optimistic concurrency control (ie read the data, modify it, and try to write).

#### *File Chunks*:

Chunk data lives in an eventually consistent bucket. This means there is no guarantee that writes to the same chunk key will be propagated to replicas in the same order — indeed, in a distributed system there is no real concept of simultaneity ([as Lamport showed](https://lamport.azurewebsites.net/pubs/time-clocks.pdf)).

#### Unique Sequence numbers
If two clients wrote to the *same* chunk key, a reader could see a mix of data from different writers depending on which replica it hits.

Currently this is solved with **write-once chunk keys**: each client's `IdGenerator` embeds its own identity (see *Unique ids*), two clients will never produce the same key. Every chunk write (whether appending or replacing an existing chunk) generates a fresh, globally unique key. The metadata is updated to point to the new key.

This gives a useful invariant: for any version of the metadata (containing the ordered list of chunk keys), every referenced chunk is immutable and will eventually be readable with exactly the data that one client intended to write. 

### KVStore interface

The backend is abstracted behind a Go interface (`KVStore`) with separate operations for metadata and chunks.
The current _implementation_ uses Riak's KV backend, but this interface could be implemented for any key value store.

### Design decisions / tradeoffs

Overall the designed is for
- Use case of single writer at a time per file:
  - for example, unlike something like GFS concurrent append is not really thought through. 
  - But this makes it convenient to use simple CAS (read, modify, write if not updated in the meantime) for metadata writes. This eliminates the need for some kind of coordination services at the cost of maybe failing some writes, orphaning chunks, and undefined effects.
- File data writes are expected to be on the hot path - tradeoff of consistency for speed writing (eg, storing a large file without the consensus overhead of strong consistency)


## Status

- `mkfs-bangfs && mount-fuse-bangfs` create and mount the filesystem, visible at `$BANGFS_MOUNTDIR`
- `make test` builds and passes 
- `make integration-test` builds and runs against a single Riak instance. Integration tests are occasionally flaky due to Riak eventual consistency propagation delays on the chunk bucket — a read immediately after a write may not see the data yet.

## Shortcomings

Conceptual

- The concept was thought up overnight. Probably nobody would actually build a production system this way.
- The strongly consistent bucket type in Riak, which the implementation depends upon, is listed as "experimental" in the Riak 2.2.3 documentation.

Various functions of a filesystem are not currently/yet implemented:

- **No file extension**: writes can append and overwrite, but growing a file past its current size via truncate is not supported.
- **No hardlinks or symlinks**.
- **No UID/GID changes**: ownership operations return ENOTSUP.
- **Directory operations are not atomic**: concurrent modifications to the same directory can conflict. The implementation detects conflicts via vclock but does not retry.
- **Efficient directory lookup**: child lookup is O(n) in the number of entries. Acceptable for small directories, slow for large ones.
- **No garbage collection**: orphaned chunks from failed writes are not cleaned up.

Tests could be added, this is a POC but would be good/interesting to add

- **benchmarking**: disk read/write throughput.
- Test multiple concurrent clients

## Requirements

- Go 1.21+
- FUSE3 (`libfuse3-dev` on Debian/Ubuntu)
- For integration testing: Docker

## Building

```bash
make build
```

Produces three binaries: `mkfs-bangfs`, `mount-fuse-bangfs`, `reformat-bangfs`.

## Testing

### Environment Variables

Set these before testing (accepted by scripts and binaries):

- `RIAK_HOST` — Riak protobuf host (default: `127.0.0.1`)
- `RIAK_PORT` — Riak protobuf port (default: `8087`)
- `RIAK_HTTP_PORT` — Riak HTTP API port (default: `8098`)
- `BANGFS_NAMESPACE` — Filesystem / bucket-type namespace prefix (required)
- `BANGFS_MOUNTDIR` — FUSE mount point directory (required)
- `RIAK_IMAGE` — Docker image for Riak (default: `bangriak`)
- `RIAK_CONTAINER` — Docker container name (default: `bangtest`)

All three binaries also accept equivalent CLI flags (`-host`, `-port`, `-namespace`, `-mount`).

### Unit test

Uses a file-backed KV store (`/tmp/`).

```bash
make test
```

### Integration test

#### Setup (one time)

Start a Riak container and initialize bucket types:

```bash
./start-riak-docker.sh
./init-riak-buckets-docker.sh
```

Verify Riak is serving:

```bash
./test-riak-rest.sh
```

#### Running

```bash
make integration-test
```

## Future work

- Benchmark read/write throughput and characterize performance bottlenecks.
- Alternative KV backends (ScyllaDB, DynamoDB).
- Retry logic for vclock conflicts in directory operations.
- Directory indexing for O(1) child lookup.
- Chunk garbage collection.
- Multi-node Riak cluster testing.