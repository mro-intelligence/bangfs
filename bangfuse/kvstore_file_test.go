package bangfuse

import (
	"testing"
)

// Unit tests for FileKVStore — no external dependencies, always runs.
// Run: go test -v ./bangfuse/

func testFileKV(t *testing.T) KVStore {
	t.Helper()

	// Each test gets its own namespace to avoid collisions
	ns := "test_" + t.Name()
	kv, err := NewFileKVStore(ns)
	if err != nil {
		t.Fatalf("NewFileKVStore: %v", err)
	}
	if err := kv.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() {
		kv.WipeBackend()
		kv.Close()
	})

	return kv
}

func TestFile_SanityCheck(t *testing.T) {
	testAllTests(t, testFileKV(t))
}
