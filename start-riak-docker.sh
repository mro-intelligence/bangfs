#!/bin/bash

# Start a Riak container for BangFS development
# Usage: ./start-riak-docker.sh [container-name]
# Example: ./start-riak-docker.sh riak-bangfs

set -e

if [[ -z "$RIAK_IMAGE" ]]; then RIAK_IMAGE="${1:-bangriak}"; fi
if [[ -z "$RIAK_CONTAINER" ]]; then RIAK_CONTAINER="${2:-bangtest}"; fi
if [[ -z "$RIAK_PORT" ]]; then RIAK_PORT=8098; fi
if [[ -z "$RIAK_HTTP_PORT" ]]; then RIAK_HTTP_PORT=8087; fi

# Check if container already exists
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
        echo "Container '$RIAK_CONTAINER' is already running."
    else
        echo "Starting existing container '$RIAK_CONTAINER'..."
        docker start "$RIAK_CONTAINER"
    fi
else
    echo "Creating and starting container '$RIAK_CONTAINER' from image '$RIAK_IMAGE'..."
    docker run --rm -d \
        --name "$RIAK_CONTAINER" \
        -p "${RIAK_PORT}:8087" \
        -p "${RIAK_HTTP_PORT}:8098" \
        "$RIAK_IMAGE"
fi

# Wait for Riak to be ready
echo "Waiting for Riak to be ready..."
while ! docker exec "$RIAK_CONTAINER" riak ping | grep -q pong; do
    sleep 5
done
echo ""
echo "Riak is ready."
echo ""
echo "Export these for BangFS:"
echo "  export RIAK_HOST=127.0.0.1"
echo "  export RIAK_PORT=${RIAK_PORT}"
exit 0
