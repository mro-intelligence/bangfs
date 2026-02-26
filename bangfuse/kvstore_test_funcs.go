package bangfuse

import (
	"syscall"
	"testing"
	"time"

	bangpb "bangfs/proto"
)

// makeTestMeta creates a simple file InodeMeta for testing.
func makeTestMeta(name string) *bangpb.InodeMeta {
	now := time.Now().UnixNano()
	return &bangpb.InodeMeta{
		Name: name, Mode: 0644 | syscall.S_IFREG,
		CtimeNs: now, MtimeNs: now, AtimeNs: now, Nlink: 1,
	}
}

// Shared test cases that run against any KVStore implementation.
// Each backend test file (kvstore_file_test.go, kvstore_integration_test.go)
// calls these with its own KVStore instance.

// Pure AI generated:

func testPutAndGetMetadata(t *testing.T, kv KVStore) {
	t.Helper()

	var inum uint64 = 9999900
	kv.DeleteMetadata(inum, nil)
	t.Cleanup(func() { kv.DeleteMetadata(inum, nil) })

	now := time.Now().UnixNano()
	meta := &bangpb.InodeMeta{
		Name:        "testfile.txt",
		ParentInode: 0,
		Mode:        0644 | syscall.S_IFREG,
		Uid:         1000,
		Gid:         1000,
		Size:        42,
		CtimeNs:     now,
		MtimeNs:     now,
		AtimeNs:     now,
		Nlink:       1,
	}

	if _, err := kv.PutMetadata(inum, meta); err != nil {
		t.Fatalf("PutMetadata: %v", err)
	}

	got, vclock, err := kv.Metadata(inum)
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if vclock == nil {
		t.Fatal("expected non-nil vclock")
	}
	if got.Name != "testfile.txt" {
		t.Errorf("Name = %q, want %q", got.Name, "testfile.txt")
	}
	if got.Size != 42 {
		t.Errorf("Size = %d, want 42", got.Size)
	}
	if got.Mode != meta.Mode {
		t.Errorf("Mode = 0%o, want 0%o", got.Mode, meta.Mode)
	}
}

func testUpdateMetadata(t *testing.T, kv KVStore) {
	t.Helper()

	var inum uint64 = 9999901
	kv.DeleteMetadata(inum, nil)
	t.Cleanup(func() { kv.DeleteMetadata(inum, nil) })

	now := time.Now().UnixNano()
	meta := &bangpb.InodeMeta{
		Name:    "before.txt",
		Mode:    0644 | syscall.S_IFREG,
		CtimeNs: now, MtimeNs: now, AtimeNs: now,
		Nlink: 1,
	}

	if _, err := kv.PutMetadata(inum, meta); err != nil {
		t.Fatalf("PutMetadata: %v", err)
	}

	_, vclock, err := kv.Metadata(inum)
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}

	meta.Name = "after.txt"
	meta.Size = 100
	if _, err := kv.UpdateMetadata(inum, meta, vclock); err != nil {
		t.Fatalf("UpdateMetadata: %v", err)
	}

	got, _, err := kv.Metadata(inum)
	if err != nil {
		t.Fatalf("Metadata after update: %v", err)
	}
	if got.Name != "after.txt" {
		t.Errorf("Name = %q, want %q", got.Name, "after.txt")
	}
	if got.Size != 100 {
		t.Errorf("Size = %d, want 100", got.Size)
	}
}

func testDeleteMetadata(t *testing.T, kv KVStore) {
	t.Helper()

	var inum uint64 = 9999902
	kv.DeleteMetadata(inum, nil)

	now := time.Now().UnixNano()
	meta := &bangpb.InodeMeta{
		Name: "deleteme.txt", Mode: 0644 | syscall.S_IFREG,
		CtimeNs: now, MtimeNs: now, AtimeNs: now, Nlink: 1,
	}
	if _, err := kv.PutMetadata(inum, meta); err != nil {
		t.Fatalf("PutMetadata: %v", err)
	}

	if err := kv.DeleteMetadata(inum, nil); err != nil {
		t.Fatalf("DeleteMetadata: %v", err)
	}

	_, _, err := kv.Metadata(inum)
	if err == nil {
		t.Fatal("expected error after delete, got nil")
	}
}

func testPutAndGetChunk(t *testing.T, kv KVStore) {
	t.Helper()

	var key uint64 = 0xDEADBEEF9999
	data := []byte("hello bangfs chunk")

	kv.DeleteChunk(key)
	t.Cleanup(func() { kv.DeleteChunk(key) })

	if err := kv.PutChunk(key, data); err != nil {
		t.Fatalf("PutChunk: %v", err)
	}

	got, err := kv.Chunk(key)
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("Chunk data = %q, want %q", got, data)
	}
}

func testDeleteChunk(t *testing.T, kv KVStore) {
	t.Helper()

	var key uint64 = 0xDEADBEEF999A
	data := []byte("delete me")

	kv.DeleteChunk(key)

	if err := kv.PutChunk(key, data); err != nil {
		t.Fatalf("PutChunk: %v", err)
	}

	if err := kv.DeleteChunk(key); err != nil {
		t.Fatalf("DeleteChunk: %v", err)
	}

	_, err := kv.Chunk(key)
	if err == nil {
		t.Fatal("expected error after delete, got nil")
	}
}

func testMetadataNotFound(t *testing.T, kv KVStore) {
	t.Helper()

	_, _, err := kv.Metadata(9999999)
	if err == nil {
		t.Fatal("expected error for nonexistent key, got nil")
	}
}

func testChunkNotFound(t *testing.T, kv KVStore) {
	t.Helper()

	_, err := kv.Chunk(0xFFFFFFFF)
	if err == nil {
		t.Fatal("expected error for nonexistent chunk, got nil")
	}
}

// Hand written by hand because I don't know what the AI did:

func testGetMetadataAfterDeleteFails(t *testing.T, kv KVStore) {
	t.Helper()

	inum := uint64(0123456)
	metadata := makeTestMeta("abc")

	t.Cleanup(func() { kv.DeleteMetadata(inum, nil) })

	if _, err := kv.PutMetadata(inum, metadata); err != nil {
		t.Errorf("PutMetadata: %v", err)
	}

	meta, _, err := kv.Metadata(inum)
	if err != nil {
		t.Errorf("Metadata: %v", err)
	}

	if meta.Name != "abc" {
		t.Errorf("Retrieved metadata is not what was stored")
	}

	if err := kv.DeleteMetadata(inum, nil); err != nil {
		t.Errorf("DeleteMetadata: %v", err)
	}

	meta, _, err = kv.Metadata(inum)
	if err == nil {
		t.Fatal("Expected an error after retrieving deleted metadata")
	}
}

func testGetNonexistentMetadata(t *testing.T, kv KVStore) {
	t.Helper()

	inum := uint64(1234567890)

	_, _, err := kv.Metadata(inum)
	if err == nil {
		t.Fatalf("Expected an error after retrieving on existent key %v", inum)
	}
}

func testAllTests(t *testing.T, kv KVStore) {
	testPutAndGetMetadata(t, kv)
	testUpdateMetadata(t, kv)
	testDeleteMetadata(t, kv)
	testPutAndGetChunk(t, kv)
	testDeleteChunk(t, kv)
	testMetadataNotFound(t, kv)
	testChunkNotFound(t, kv)
	testGetMetadataAfterDeleteFails(t, kv)
	testGetMetadataAfterDeleteFails(t, kv)
}
