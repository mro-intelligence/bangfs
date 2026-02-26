#!/bin/bash

# Test Riak bucket types via REST API
# Usage: ./test-riak-rest.sh [namespace]
# Requires: RIAK_HOST and RIAK_HTTP_PORT env vars (or defaults to 127.0.0.1:8098)

if [[ -z "$RIAK_HOST" ]]; then RIAK_HOST="127.0.0.1"; fi
if [[ -z "$RIAK_HTTP_PORT" ]]; then RIAK_HTTP_PORT="8098"; fi
if [[ -z "$BANGFS_NAMESPACE" ]]; then BANGFS_NAMESPACE="${1:-test}"; fi

BASE="http://${RIAK_HOST}:${RIAK_HTTP_PORT}"
META_TYPE="${BANGFS_NAMESPACE}_bangfs_metadata"
CHUNK_TYPE="${BANGFS_NAMESPACE}_bangfs_chunks"
COUNTER_TYPE="${BANGFS_NAMESPACE}_bangfs_counters"

PASS=0
FAIL=0

check() {
    local label="$1"
    local expected="$2"
    local actual_code="$3"
    local body="$4"
    if [[ "$actual_code" -eq "$expected" ]]; then
        echo "  PASS: $label (HTTP $actual_code)"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $label (expected HTTP $expected, got $actual_code)"
        echo "        body: $body"
        FAIL=$((FAIL + 1))
    fi
}

# run_curl <label> <expected_code> <curl args...>
run_curl() {
    local label="$1"; shift
    local expected="$1"; shift
    local tmpfile
    tmpfile=$(mktemp)
    local code
    code=$(curl -s -w '%{http_code}' -o "$tmpfile" "$@")
    local body
    body=$(cat "$tmpfile")
    rm -f "$tmpfile"
    check "$label" "$expected" "$code" "$body"
}

echo "=== Testing Riak REST API at $BASE ==="
echo "Namespace: $BANGFS_NAMESPACE"
echo ""

# --- Metadata bucket type ---
echo "--- Metadata bucket type: $META_TYPE ---"

# Random key to avoid sibling values (we are just testing basic CRUD)
TESTKEY="testkey_$RANDOM"

run_curl "PUT metadata" 204 \
    -X PUT "${BASE}/types/${META_TYPE}/buckets/testbucket/keys/$TESTKEY" \
    -H 'Content-Type: application/octet-stream' \
    -d 'metadata-test-value'

run_curl "GET metadata" 200 \
    "${BASE}/types/${META_TYPE}/buckets/testbucket/keys/$TESTKEY"

# Verify value
VAL=$(curl -s "${BASE}/types/${META_TYPE}/buckets/testbucket/keys/$TESTKEY")
if [[ "$VAL" == "metadata-test-value" ]]; then
    echo "  PASS: metadata value matches"
    PASS=$((PASS + 1))
else
    echo "  FAIL: metadata value mismatch (got: $VAL)"
    FAIL=$((FAIL + 1))
fi

run_curl "DELETE metadata" 204 \
    -X DELETE "${BASE}/types/${META_TYPE}/buckets/testbucket/keys/$TESTKEY"

run_curl "GET metadata after delete" 404 \
    "${BASE}/types/${META_TYPE}/buckets/testbucket/keys/$TESTKEY"

echo ""

# --- Chunk bucket type ---
echo "--- Chunk bucket type: $CHUNK_TYPE ---"

run_curl "PUT chunk" 204 \
    -X PUT "${BASE}/types/${CHUNK_TYPE}/buckets/testbucket/keys/$TESTKEY" \
    -H 'Content-Type: application/octet-stream' \
    -d 'chunk-binary-data'

run_curl "GET chunk" 200 \
    "${BASE}/types/${CHUNK_TYPE}/buckets/testbucket/keys/$TESTKEY"

run_curl "DELETE chunk" 204 \
    -X DELETE "${BASE}/types/${CHUNK_TYPE}/buckets/testbucket/keys/$TESTKEY"

run_curl "GET chunk after delete" 404 \
    "${BASE}/types/${CHUNK_TYPE}/buckets/testbucket/keys/$TESTKEY"

echo ""

# --- Counter bucket type (CRDT) ---
echo "--- Counter bucket type: $COUNTER_TYPE ---"

run_curl "POST counter increment" 204 \
    -X POST "${BASE}/types/${COUNTER_TYPE}/buckets/testbucket/datatypes/test_counter" \
    -H 'Content-Type: application/json' \
    -d '{"increment": 1}'

run_curl "POST counter increment again" 204 \
    -X POST "${BASE}/types/${COUNTER_TYPE}/buckets/testbucket/datatypes/test_counter" \
    -H 'Content-Type: application/json' \
    -d '{"increment": 1}'

run_curl "GET counter" 200 \
    "${BASE}/types/${COUNTER_TYPE}/buckets/testbucket/datatypes/test_counter"

VAL=$(curl -s "${BASE}/types/${COUNTER_TYPE}/buckets/testbucket/datatypes/test_counter")
echo "  Counter value: $VAL"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[[ "$FAIL" -eq 0 ]]
