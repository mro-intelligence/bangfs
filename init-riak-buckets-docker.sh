#!/bin/bash

# Initialize Riak bucket types for BangFS (Docker)
# Usage: ./init-riak-buckets-docker.sh [namespace] [container-name]
# Example: ./init-riak-buckets-docker.sh myfs riak-bangfs

set -e

if [[ -z "$BANGFS_NAMESPACE" ]]; then BANGFS_NAMESPACE="${1:-test}"; fi
if [[ -z "$RIAK_CONTAINER" ]]; then RIAK_CONTAINER="${2:-riak-bangfs}"; fi

echo "Initializing BangFS bucket types with prefix: $BANGFS_NAMESPACE"
echo "Docker container: $RIAK_CONTAINER"

create_bucket_type() {
    local name="$1"
    local props="$2"

    echo " ==== Creating bucket type '$name'... ===="
    docker exec "$RIAK_CONTAINER" riak-admin bucket-type create "$name" "$props"
    docker exec "$RIAK_CONTAINER" riak-admin bucket-type activate "$name"
}

# For single-node testing, use n_val=1, w=1, r=1
# For production, increase n_val and use consistent=true with proper quorum
create_bucket_type "${BANGFS_NAMESPACE}_bangfs_metadata" '{"props":{"n_val":1,"w":1,"r":1}}'
create_bucket_type "${BANGFS_NAMESPACE}_bangfs_chunks" '{"props":{"n_val":1,"w":1,"r":1}}'

echo "Bucket types initialized successfully!"
