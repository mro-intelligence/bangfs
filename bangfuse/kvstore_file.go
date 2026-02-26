package bangfuse

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"google.golang.org/protobuf/proto"

	bangpb "bangfs/proto"
)

// FileKVStore implements KVStore using flat files under /tmp/bangfs_<namespace>/.
// Layout:
//
//	/tmp/bangfs_<namespace>/metadata/<inode>         — protobuf-encoded InodeMeta
//	/tmp/bangfs_<namespace>/metadata/<inode>.vclock   — 8-byte little-endian version counter
//	/tmp/bangfs_<namespace>/chunks/<hex_hash>         — raw chunk bytes
type FileKVStore struct {
	namespace   string
	baseDir     string
	metadataDir string
	chunkDir    string
}

func NewFileKVStore(namespace string) (*FileKVStore, error) {
	base := filepath.Join("/tmp", "bangfs_"+namespace)
	kv := &FileKVStore{
		namespace:   namespace,
		baseDir:     base,
		metadataDir: filepath.Join(base, "metadata"),
		chunkDir:    filepath.Join(base, "chunks"),
	}
	if err := os.MkdirAll(kv.metadataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create metadata dir: %w", err)
	}
	if err := os.MkdirAll(kv.chunkDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create chunk dir: %w", err)
	}
	if err := kv.Connect(); err != nil {
		return nil, err
	}
	return kv, nil
}

func (kv *FileKVStore) Connect() error {
	return nil
}

func (kv *FileKVStore) Close() error {
	return nil
}

func (kv *FileKVStore) InitBackend() error {
	existing, _, err := kv.Metadata(0)
	if err == nil && existing != nil {
		return fmt.Errorf("filesystem already exists (inode 0 found). Use WipeBackend() first")
	}

	now := time.Now().UnixNano()
	rootDir := &bangpb.InodeMeta{
		Name: "", ParentInode: 0,
		Mode:         0755 | syscall.S_IFDIR,
		Uid:          0,
		Gid:          0,
		CtimeNs:      now,
		MtimeNs:      now,
		AtimeNs:      now,
		ChildEntries: []*bangpb.ChildEntry{},
		Nlink:        2,
	}

	_, err = kv.PutMetadata(0, rootDir)
	return err
}

// Metadata paths

func (kv *FileKVStore) metaPath(key uint64) string {
	return filepath.Join(kv.metadataDir, fmt.Sprintf("%d", key))
}

func (kv *FileKVStore) vclockPath(key uint64) string {
	return filepath.Join(kv.metadataDir, fmt.Sprintf("%d.vclock", key))
}

func (kv *FileKVStore) chunkPath(key uint64) string {
	return filepath.Join(kv.chunkDir, fmt.Sprintf("%016x", key))
}

// readVclock reads the current vclock (version counter) for a metadata key.
func (kv *FileKVStore) readVclock(key uint64) ([]byte, error) {
	data, err := os.ReadFile(kv.vclockPath(key))
	if err != nil {
		return nil, err
	}
	return data, nil
}

// bumpVclock increments the vclock and returns the new value.
func (kv *FileKVStore) bumpVclock(key uint64) ([]byte, error) {
	var version uint64 = 1
	old, err := kv.readVclock(key)
	if err == nil && len(old) == 8 {
		version = binary.LittleEndian.Uint64(old) + 1
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, version)
	if err := os.WriteFile(kv.vclockPath(key), buf, 0644); err != nil {
		return nil, err
	}
	return buf, nil
}

// Metadata CRUD

func (kv *FileKVStore) PutMetadata(key uint64, newMeta *bangpb.InodeMeta) ([]byte, error) {
	// Fail if key already exists (matches Riak IfNoneMatch behavior)
	if _, err := os.Stat(kv.metaPath(key)); err == nil {
		return nil, fmt.Errorf("key already exists: %d", key)
	}

	data, err := proto.Marshal(newMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := os.WriteFile(kv.metaPath(key), data, 0644); err != nil {
		return nil, fmt.Errorf("failed to write metadata: %w", err)
	}

	vclock, err := kv.bumpVclock(key)
	if err != nil {
		return nil, fmt.Errorf("failed to write vclock: %w", err)
	}
	return vclock, nil
}

func (kv *FileKVStore) Metadata(key uint64) (*bangpb.InodeMeta, []byte, error) {
	data, err := os.ReadFile(kv.metaPath(key))
	if err != nil {
		return nil, nil, fmt.Errorf("key not found: %d", key)
	}

	meta := &bangpb.InodeMeta{}
	if err := proto.Unmarshal(data, meta); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	vclock, err := kv.readVclock(key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read vclock: %w", err)
	}

	return meta, vclock, nil
}

func (kv *FileKVStore) UpdateMetadata(key uint64, newMeta *bangpb.InodeMeta, vclockIn []byte) ([]byte, error) {
	// Check vclock matches (simulates Riak IfNotModified)
	current, err := kv.readVclock(key)
	if err != nil {
		return nil, fmt.Errorf("key not found: %d", key)
	}
	if len(vclockIn) == 8 && len(current) == 8 {
		want := binary.LittleEndian.Uint64(vclockIn)
		have := binary.LittleEndian.Uint64(current)
		if want != have {
			return nil, fmt.Errorf("vclock mismatch: expected %d, got %d (concurrent modification)", want, have)
		}
	}

	data, err := proto.Marshal(newMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := os.WriteFile(kv.metaPath(key), data, 0644); err != nil {
		return nil, fmt.Errorf("failed to write metadata: %w", err)
	}

	vclock, err := kv.bumpVclock(key)
	if err != nil {
		return nil, fmt.Errorf("failed to write vclock: %w", err)
	}
	return vclock, nil
}

func (kv *FileKVStore) DeleteMetadata(key uint64, vclockIn []byte) error {
	// If vclock provided, verify it matches current version (optimistic concurrency)
	if vclockIn != nil {
		current, err := kv.readVclock(key)
		if err != nil {
			return fmt.Errorf("failed to read vclock for concurrency check: %w", err)
		}
		if len(current) == 8 && len(vclockIn) == 8 {
			cur := binary.LittleEndian.Uint64(current)
			in := binary.LittleEndian.Uint64(vclockIn)
			if cur != in {
				return fmt.Errorf("vclock mismatch: current=%d, provided=%d (concurrent modification)", cur, in)
			}
		}
	}
	os.Remove(kv.vclockPath(key))
	if err := os.Remove(kv.metaPath(key)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete metadata: %w", err)
	}
	return nil
}

// Chunk CRUD

func (kv *FileKVStore) PutChunk(key uint64, data []byte) error {
	if err := os.WriteFile(kv.chunkPath(key), data, 0644); err != nil {
		return fmt.Errorf("failed to write chunk: %w", err)
	}
	return nil
}

func (kv *FileKVStore) Chunk(key uint64) ([]byte, error) {
	data, err := os.ReadFile(kv.chunkPath(key))
	if err != nil {
		return nil, fmt.Errorf("chunk not found: %016x", key)
	}
	return data, nil
}

func (kv *FileKVStore) DeleteChunk(key uint64) error {
	if err := os.Remove(kv.chunkPath(key)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete chunk: %w", err)
	}
	return nil
}

// WipeBackend deletes all keys (files) in the metadata and chunk directories.
func (kv *FileKVStore) WipeBackend() error {
	for _, dir := range []string{kv.metadataDir, kv.chunkDir} {
		if !strings.HasPrefix(dir, "/tmp/") {
			return fmt.Errorf("refusing to wipe %q: not under /tmp", kv.baseDir)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("failed to read dir %s: %w", dir, err)
		}
		for _, e := range entries {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				return fmt.Errorf("failed to delete %s: %w", e.Name(), err)
			}
		}
	}
	return nil
}
