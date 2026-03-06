#!/bin/bash

# Start a Riak container for BangFS development
# Usage: ./start-riak-docker.sh [container-name]
# Example: ./start-riak-docker.sh riak-bangfs

set -e

if [[ -z "$RIAK_IMAGE" ]]; then RIAK_IMAGE="${1:-ghcr.io/gosub2k/riak-docker}"; fi
if [[ -z "$RIAK_CONTAINER" ]]; then RIAK_CONTAINER="${2:-riak}"; fi
if [[ -z "$RIAK_PB_PORT" ]]; then RIAK_PB_PORT=8087; fi
if [[ -z "$RIAK_HTTP_PORT" ]]; then RIAK_HTTP_PORT=8098; fi

# Check if container already exists
if docker ps -a --format '{{.Names}}' | grep -q "^${RIAK_CONTAINER}$"; then
    if docker ps --format '{{.Names}}' | grep -q "^${RIAK_CONTAINER}$"; then
        echo "Container '$RIAK_CONTAINER' is already running."
    else
        echo "Starting existing container '$RIAK_CONTAINER'..."
        docker start "$RIAK_CONTAINER"
    fi
else
    echo "Creating and starting container '$RIAK_CONTAINER' from image '$RIAK_IMAGE'..."
    docker run --rm -d \
        --name "$RIAK_CONTAINER" \
        -p "${RIAK_PB_PORT}:8087" \
        -p "${RIAK_HTTP_PORT}:8098" \
        --entrypoint bash \
        "$RIAK_IMAGE" \
        -c "sed -i 's|listener.http.internal = 127.0.0.1:|listener.http.internal = 0.0.0.0:|' /opt/riak/etc/riak.conf && \
            sed -i 's|listener.protobuf.internal = 127.0.0.1:|listener.protobuf.internal = 0.0.0.0:|' /opt/riak/etc/riak.conf && \
            exec /usr/local/bin/k8s-entrypoint.sh"
fi

# Wait for Riak to be ready
echo "Waiting for Riak to be ready..."
while ! docker exec "$RIAK_CONTAINER" riak ping 2>/dev/null | grep -q pong; do
    sleep 5
done
echo ""
echo "Riak is ready."
echo ""
echo "Export these for BangFS:"
echo "  export RIAK_HOST=127.0.0.1"
echo "  export RIAK_PB_PORT=${RIAK_PB_PORT}"
echo "  export RIAK_HTTP_PORT=${RIAK_HTTP_PORT}"
exit 0
