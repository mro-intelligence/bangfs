//go:build integration

// Integration tests for RiakKVStore — requires a running Riak instance.
// Set RIAK_HOST, RIAK_PORT, BANGFS_NAMESPACE env vars or use defaults.
// Run: go test -tags=integration -v ./bangfuse/

package bangfuse

import (
	"os"
	"strconv"
	"testing"
)

func testRiakKV(t *testing.T) KVStore {
	t.Helper()

	host := os.Getenv("RIAK_HOST")
	portStr := os.Getenv("RIAK_PORT")
	ns := os.Getenv("BANGFS_NAMESPACE")

	if host == "" || portStr == "" || ns == "" {
		t.Fatalf("set RIAK_HOST, RIAK_PORT, BANGFS_NAMESPACE to run Riak tests")
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("invalid RIAK_PORT: %v", err)
	}

	kv, err := NewRiakKVStore(host, uint16(port), ns)
	if err != nil {
		t.Fatalf("NewRiakKVStore: %v", err)
	}
	t.Cleanup(func() { kv.Close() })

	return kv
}

func TestRiak_SanityCheck(t *testing.T) {
	testAllTests(t, testRiakKV(t))
}
