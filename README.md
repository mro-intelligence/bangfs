# BangFS

## Motivation

- Build a novel distributed filesystem out of available parts, insipired by GFS.
- Experimental, intended to test distributed systems concepts.
- For read-oriented storage, ie, backups, data.
- Minimal components exposed: Only a key value store and a FUSE client program 
  - (And a file system checker...and... this is the motivation, not the end result)
- Ultimately may be useful for utilizing free disk space across heterogenous machines

## High Level Goals

- Use a single off the shelf key value store service for all data and metadata
  - file data is stored in chunks, addressed by sequence number, and writes are eventually consistent
  - metadata is strongly consistent (reads are guaranteed to see all previous writes): acheive this by enabling a *strongly consistent* property on these keys
- Avoid explicit 'leader-follower' patterns
- Try not to run any other services (coordinators, monitors, metadata servers, managers...)
- Be scaleable, in theory as much as the key value store can handle.
- Handle concurrent writes with optimistic concurrency control (CAS operations)

## Tradeoffs: 
- metadata consistent -> writes to metadata are slower than to file data due to consensus algorithm
- file data eventually consistent -> writes are faster than to metadata
- If a read operation occurs soon after a write, this difference means there is no guarantee that  file chunks listed in the internal inode metadata will have completed writing. Accept this and possibly other edge cases.

## Low Level Design

 - Use [Riak](https://docs.riak.com/riak/kv/latest/index.html) as the key/value store. (this is a POC which is not intended to be a production system and should work with any decent key value store.)
 - The key value store is abstracted with a Go interface so it will be easy to create a different backend implementation if this is ever useful (eg, DynamaDB, ScyllaDB).

## Design issues and shortcomings

_Probably nobody would actually build a production system this way._ 

- There are many distributed filesystems out there, but few (none that I could find with a web search) use a key value store for both metadata and file data: This is not necessarily a bad thing. It speaks also to organizational dynamics, design requirements and established solutions already being out there. The concept for this filesystem was thought up literally overnight.
- The strongly consistent bucket type propery in Riak, which the implementaion depends upon, is listed as "experimental" in the Riak 2.2.3 documentation.
- 

## Status

- `make test` builds successfully and (hermetic/unit) tests pass.
- `make integration-test` also builds successfully against a single, non-distributed Riak KVStore instance.
- `mkfs-bangfs && mount-fuse-bangfs` create and mount the fs, its visible in the `$BANGFS_MOUNTDIR`. (See below for environment var setup.)

## TODOS

- Multiple
- 

## Requirements

- Go 1.21+
- For integration testing: Docker

## Building

Produces three binaries: `mkfs-bangfs`, `mount-fuse-bangfs`, `reformat-bangfs`.

## Testing

As well as go unit test, this target calls a python script to test basic filesystem operations like reading/writing, creating files, etc.

### Environment Variables

Set these environment variables before testing (accepted by the scripts and binaries):

- `RIAK_HOST` — Riak protobuf host address (default: `127.0.0.1` in scripts)
- `RIAK_PORT` — Riak protobuf port (default: `8087`)
- `RIAK_HTTP_PORT` — Riak HTTP API port (default: `8098`)
- `BANGFS_NAMESPACE` — Filesystem / bucket-type namespace prefix (required)
- `BANGFS_MOUNTDIR` — FUSE mount point directory (required)
- `RIAK_IMAGE` — Docker image name for Riak (default: `bangriak`)
- `RIAK_CONTAINER` — Docker container name for Riak (default: `bangtest`)

All three binaries also accept equivalent CLI flags (`-host`, `-port`, `-namespace`, `-mount`).

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
