# BangFS

## Motivation

Build a novel distributed filesystem out of available parts, insipired by GFS.
Intended to test distributed systems concepts.

## Design Goals

- Be as homogenous as possible, avoid explicit 'leader-follower' patterns and use as few different services as possible: depends on only client programs, a backend key value store, and consistency properties of the key value store.
- Be scaleable, in theory as much as the key value store can handle.
- Handle concurrent writes with optimistic concurrency control (CAS operations), without any explicit locking mechanisem, but don't design for many concurrent writers: writes that would result in an inconsistent state may fail.
- Be useable at least at small scale, with latency and performance characteristics at least as good as spinning disks.

## High Level Concept

- store chunks of files in a distributed, eventually consistent, key value store.
- store file metadata in a key value store with a *strong consistency* property enabled on only the metadata keys.
- trade offs: 
  - metadata (inode data) will be consistent with itself, indeed *strongly consistent*, but with some performance cost since the consistency property in distributed systems often requires a distributed consensus algorithm to be used.
  - file data will be eventually consistent. with some performance benefit when writing file contents, as compared to metadata updates. 
  - If a read operation occurs soon after a write, eventual consistency means that there will be no guarantee that file chunks listed in the internal inode metadata will exist. Unlike the metatdata theres no guarantee when writes to file chunks will be visible to a read operation.

## Design issues/shortcomings/TODOS

_Probably nobody would actually build a production system this way._ 

- There are many distributed filesystems out there, but few (none that I could find with a web search) use a key value store for both metadata and file data.
- This is not necessarily a bad thing. It speaks also to organizational dynamics, design requirements and established solutions already being out there, that are maintained by teams of people.

## Status

`make test` builds successfully and (hermetic/unit) tests pass.
`make test` builds successfully and (hermetic/unit) tests pass.

## Requirements

- Go 1.21+
- For integration testing: Docker

## Building

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

## Testing

As well as go unit test, this target calls a python script to test basic filesystem operations like reading/writing, creating files, etc.

### Unit test

The unit test uses a dummy key-value store that just stores keys/values as files in `/tmp/`. 

```bash
make test
```

### Integration test

#### Setup (one time)

For the integration test, you need to set up a Riak instance to test against.
You'll need a docker container that has riak - set the `RIAK_IMAGE` env var. Start the container and initialize the bucket types for chunk data and for metadata. The scripts:

```bash
./start-riak-docker.sh
./init-riak-buckets-docker.sh
````
  Should help. You can also run:

```bash
./test-riak-rest.sh
```
To make sure the docker riak instance is serving requests.

#### Running the integration test

(Assuming you have a running Riak instance!), set the RIAK_HOST, RIAK_PORT, RIAK_HTTP_PORT, BANGFS_NAMESPACE env variables and do the integration test

```bash
make integration-test
```
