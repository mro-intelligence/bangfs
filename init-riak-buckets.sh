#!/bin/bash

# Initialize Riak bucket types for BangFS
# Usage: ./init-riak-buckets.sh <container-name> [namespace]
# Example: ./init-riak-buckets.sh riak-bangfs myfs

set -e

BANGFS_NAMESPACE="${1:-test}"
K8S_NAMESPACE="${2:-bangfs-riak}"
K8S_SERVICE="riak"

echo "Initializing BangFS bucket types with prefix: $BANGFS_NAMESPACE"
echo "Kubernetes namespace: $K8S_NAMESPACE"
echo "Kubernetes service: $K8S_SERVICE"

create_bucket_type() {
    local name="$1"
    local props="$2"

    echo " ==== Creating bucket type '$name'... ===="
    kubectl exec -n "$K8S_NAMESPACE" -it svc/"$K8S_SERVICE" -- riak-admin bucket-type create "$name" "$props"
    kubectl exec -n "$K8S_NAMESPACE" -it svc/"$K8S_SERVICE" -- riak-admin bucket-type activate "$name"
}

# For single-node testing, use n_val=1, w=1, r=1
# For production, increase n_val and use consistent=true with proper quorum
create_bucket_type "${BANGFS_NAMESPACE}_bangfs_metadata" '{"props":{"n_val":1,"w":1,"r":1}}'
create_bucket_type "${BANGFS_NAMESPACE}_bangfs_chunks" '{"props":{"n_val":1,"w":1,"r":1}}'

echo "Bucket types initialized successfully!"
