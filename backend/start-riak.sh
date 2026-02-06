#!/bin/bash
# Start Riak and initialize BangFS buckets
# Usage: ./start-riak.sh <namespace>
# Example: ./start-riak.sh foobar
#   Creates buckets: foobar_bangfs_metadata, foobar_bangfs_chunks

set -e

NAMESPACE="${1:-bangfs}"
CONTAINER_NAME="riak-bangfs"
IMAGE_NAME="riak-test"
HTTP_PORT=8098
PB_PORT=8087

echo "=== Starting Riak for BangFS namespace: $NAMESPACE ==="

# Check if container already running
if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Container $CONTAINER_NAME is already running"
else
    # Remove stopped container if exists
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

    echo "Starting Riak container..."
    docker run -d \
        --name "$CONTAINER_NAME" \
        --ulimit nofile=262144:262144 \
        -p ${HTTP_PORT}:8098 \
        -p ${PB_PORT}:8087 \
        -v riak-data:/opt/riak/data \
        "$IMAGE_NAME"
fi

# Wait for Riak to be ready
echo "Waiting for Riak to start..."
MAX_WAIT=60
WAIT_COUNT=0
until curl -s "http://localhost:${HTTP_PORT}/ping" 2>/dev/null | grep -q "OK"; do
    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 2))
    if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
        echo "ERROR: Riak did not start within ${MAX_WAIT} seconds"
        echo "Check logs with: docker logs $CONTAINER_NAME"
        exit 1
    fi
    echo "  Waiting... (${WAIT_COUNT}s)"
done
echo "Riak is ready!"

# Create bucket types
META_BUCKET="${NAMESPACE}_bangfs_metadata"
CHUNK_BUCKET="${NAMESPACE}_bangfs_chunks"

echo "Creating bucket types..."

# Check if bucket types already exist, create if not
create_bucket_type() {
    local name="$1"
    local props="$2"

    # Check if bucket type exists
    if docker exec "$CONTAINER_NAME" riak-admin bucket-type status "$name" >/dev/null 2>&1; then
        echo "  Bucket type '$name' already exists"
    else
        echo "  Creating bucket type '$name'..."
        docker exec "$CONTAINER_NAME" riak-admin bucket-type create "$name" "$props"
        docker exec "$CONTAINER_NAME" riak-admin bucket-type activate "$name"
    fi
}

# Metadata bucket: strong consistency for filesystem metadata
create_bucket_type "$META_BUCKET" '{"props":{"allow_mult":false}}'

# Chunk bucket: optimized for reads, durable writes (n_val=4, w=3, r=1)
create_bucket_type "$CHUNK_BUCKET" '{"props":{"n_val":3,"w":"quorum","r":1,"allow_mult":false}}'

echo ""
echo "=== Riak is ready for BangFS ==="
echo ""
echo "Container:  $CONTAINER_NAME"
echo "HTTP API:   http://localhost:${HTTP_PORT}"
echo "Protocol Buffers: localhost:${PB_PORT}"
echo ""
echo "Buckets:"
echo "  Metadata: $META_BUCKET"
echo "  Chunks:   $CHUNK_BUCKET"
echo ""
echo "To stop:    docker stop $CONTAINER_NAME"
echo "To logs:    docker logs -f $CONTAINER_NAME"
echo ""
