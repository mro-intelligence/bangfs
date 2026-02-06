#!/bin/bash
set -e

# Set ulimits required by Riak
ulimit -n 262144 2>/dev/null || echo "Warning: Could not set open files limit (run container with --ulimit nofile=262144:262144)"
ulimit -u 65536 2>/dev/null || echo "Warning: Could not set max user processes"

# Find Riak directory (support both multi-stage and single-stage builds)
if [ -d "/opt/riak/bin" ]; then
    RIAK_DIR="/opt/riak"
elif [ -d "/opt/riak/_build/rel/rel/riak/bin" ]; then
    RIAK_DIR="/opt/riak/_build/rel/rel/riak"
else
    echo "ERROR: Cannot find Riak installation"
    exit 1
fi

RIAK_CONF="$RIAK_DIR/etc/riak.conf"
LOG_DIR="$RIAK_DIR/log"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Create log file symlinks to stdout/stderr for Docker
ln -sf /dev/stdout "$LOG_DIR/console.log"
ln -sf /dev/stderr "$LOG_DIR/error.log"

# Configure Riak to listen on all interfaces for Docker networking
if [ -f "$RIAK_CONF" ]; then
    sed -i 's/^listener.http.internal = 127.0.0.1:/listener.http.internal = 0.0.0.0:/' "$RIAK_CONF"
    sed -i 's/^listener.protobuf.internal = 127.0.0.1:/listener.protobuf.internal = 0.0.0.0:/' "$RIAK_CONF"
    # Also update nodename to use container hostname
    HOSTNAME=$(hostname -f 2>/dev/null || hostname)
    sed -i "s/^nodename = .*/nodename = riak@${HOSTNAME}/" "$RIAK_CONF"
fi

# Export PATH to include Riak binaries
export PATH="$RIAK_DIR/bin:$PATH"

# If running default command, use foreground mode for Docker
if [ "$1" = "riak" ] && [ "$2" = "console" ]; then
    exec riak foreground
else
    exec "$@"
fi
