# BangFS

FUSE filesystem backed by Riak KV, intended to test distributed systems concepts.

## Status

Directory operations and metadata are fully working. File read/write (chunk I/O) is not yet implemented — files can be created and deleted but have no content.

**Working:**
- Directory tree: `mkdir`, `rmdir`, `ls`, `cd`, rename
- File metadata: `touch`, `rm`, `stat`, `chmod`, `chown`, timestamps
- Inode allocation and POSIX attribute storage
- Riak backend with optimistic concurrency control (CAS)
- CLI tools: `mkfs-bangfs`, `mount-fuse-bangfs`, `reformat-bangfs`
- Daemon mode, signal handling, debug tracing

**Not yet implemented:**
- File read (returns empty)
- File write (returns EROFS)
- Chunk creation, retrieval, and garbage collection
- Truncate (metadata field exists, no chunk handling)

## Requirements

- Go 1.21+
- Riak KV cluster (protocol buffers port, default 8087)
- `protoc` (only if regenerating protobuf code)
  - Ubuntu/Debian: `apt install protobuf-compiler`
  - macOS: `port install protobuf`

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

All commands accept `-host`, `-port`, `-namespace` flags or the environment variables `BANGFS_HOST`, `BANGFS_PORT`, `BANGFS_NAMESPACE`.

### Riak setup

Create bucket types on a Riak node before first use:

```bash
# Metadata bucket: strong consistency
riak-admin bucket-type create <ns>_bangfs_metadata '{"props":{"consistent":true}}'
riak-admin bucket-type activate <ns>_bangfs_metadata

# Chunk bucket: eventual consistency, optimized for reads
riak-admin bucket-type create <ns>_bangfs_chunks '{"props":{"n_val":4,"w":3,"r":1}}'
riak-admin bucket-type activate <ns>_bangfs_chunks
```

### Initialize a filesystem

```bash
mkfs-bangfs -host riak1 -namespace myfs
```

### Mount

```bash
mount-fuse-bangfs -host riak1 -namespace myfs -mount /mnt/bangfs
```

Options: `-daemon` (background), `-trace` (debug output).

### Destroy a filesystem

```bash
reformat-bangfs -host riak1 -namespace myfs        # interactive confirmation
reformat-bangfs -host riak1 -namespace myfs -force  # skip confirmation
```

## Architecture

BangFS uses Riak KV as backend with two bucket types per namespace:

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

Metadata is serialized with protocol buffers (`proto/metadata.proto`). Chunks are content-addressed by FNV-1a hash.

## Design

### Metadata Store (implemented)

- **Consistency:** Strongly consistent (Riak `consistent=true`)
- **Addressing:** Keyed by inode number
- **Concurrency:** Version field on each inode enables optimistic concurrency control

### Chunk Store (backend implemented, file I/O not wired up)

- **Consistency:** Eventually consistent (N=4, W=3, R=1)
- **Addressing:** Content-addressed by FNV-1a hash
- **Immutability:** Chunks are never modified, only created

### Planned Write Protocol

1. Read the metadata for a file (returns list of chunk IDs + offsets)
2. Determine if write modifies existing chunk or appends new chunks
3. If modifying: read chunk, modify locally, compute new hash, upload new chunk
4. Update chunk ID list in metadata, write with strong consistency

### Planned Garbage Collection

Orphaned chunks can occur when concurrent writes to the same file cause one update to overwrite the other. A GC process would periodically:

1. Scan all chunk IDs referenced in metadata
2. Identify unreferenced chunks (with safe time delta)
3. Remove orphaned chunks

## Tradeoffs

### Pros

- **Metadata consistency** — utilities can repair chunk issues
- **Chunk performance** — no consistency overhead on chunk read/write
- **Scalability** — inherits Riak's scalability and durability

### Cons

- **Concurrent write race condition** — two clients modifying the same chunk can orphan one version
- **Metadata write rate** — proportional to write ops, slower due to strong consistency

Not recommended for multiple concurrent appenders to the same file or heavy random writes with multiple clients.
