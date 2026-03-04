package bangfuse

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	lru "github.com/hashicorp/golang-lru/v2"
	"google.golang.org/protobuf/proto"

	bangpb "bangfs/proto"
)

const scyllaMaxQueueLen = 1000
const scyllaNumWriteWorkers = 8

// writeOp represents an async write to be executed by a worker.
type writeOp struct {
	query    string
	args     []interface{}
	chunkKey *uint64 // non-nil for chunk writes; used to mark cache entry clean
}

// ScyllaKVStore implements KVStore using ScyllaDB (CQL) as the backend.
type ScyllaKVStore struct {
	keyspace   string
	hosts      []string
	session    *gocql.Session
	chunkCache *lru.Cache[uint64, chunkEntry]
	writeQueue chan writeOp
	writeWg    sync.WaitGroup
	errChan    chan error
}

// NewScyllaKVStore creates a new ScyllaDB-backed KVStore.
// hosts is a list of ScyllaDB contact points (e.g. ["192.168.1.1", "192.168.1.2"]).
// namespace is used to derive the keyspace name.
// cacheSize is the chunk cache size in bytes.
func NewScyllaKVStore(hosts []string, namespace string, cacheSize int, errorWriter io.Writer) (*ScyllaKVStore, error) {
	kv := &ScyllaKVStore{
		keyspace:   namespace + "_bangfs",
		hosts:      hosts,
		writeQueue: make(chan writeOp, scyllaMaxQueueLen),
		errChan:    make(chan error),
	}

	if err := kv.Connect(); err != nil {
		return nil, err
	}

	chunkCache, err := lru.NewWithEvict[uint64, chunkEntry](max(1, cacheSize/int(GetChunkSize())), kv.onEvict)
	if err != nil {
		return nil, err
	}
	kv.chunkCache = chunkCache

	for i := 0; i < scyllaNumWriteWorkers; i++ {
		go kv.scyllaWriteWorker()
	}

	go func() {
		for e := range kv.errChan {
			fmt.Fprintf(errorWriter, "%v", e)
		}
	}()

	return kv, nil
}

func (kv *ScyllaKVStore) scyllaWriteWorker() {
	kv.writeWg.Add(1)
	defer kv.writeWg.Done()
	for op := range kv.writeQueue {
		if err := kv.session.Query(op.query, op.args...).Exec(); err != nil {
			kv.errChan <- fmt.Errorf("async write failed: %w", err)
		} else if op.chunkKey != nil {
			kv.markClean(*op.chunkKey)
		}
	}
}

func (kv *ScyllaKVStore) dirty(chunkkey uint64) bool {
	if entry, ok := kv.chunkCache.Peek(chunkkey); ok {
		return entry.dirty
	}
	return false
}

func (kv *ScyllaKVStore) markDirty(chunkkey uint64) {
	if entry, ok := kv.chunkCache.Peek(chunkkey); ok {
		entry.dirty = true
		kv.chunkCache.Add(chunkkey, entry)
	}
}

func (kv *ScyllaKVStore) markClean(chunkkey uint64) {
	if entry, ok := kv.chunkCache.Peek(chunkkey); ok {
		entry.dirty = false
		kv.chunkCache.Add(chunkkey, entry)
	}
}

func (kv *ScyllaKVStore) onEvict(key uint64, entry chunkEntry) {
	if entry.dirty {
		k := key
		kv.enqueue(
			`INSERT INTO chunks (key, data) VALUES (?, ?)`,
			[]interface{}{int64(key), entry.data},
			&k,
		)
	}
}

func (kv *ScyllaKVStore) enqueue(query string, args []interface{}, chunkKey *uint64) error {
	select {
	case kv.writeQueue <- writeOp{query: query, args: args, chunkKey: chunkKey}:
		return nil
	default:
		return fmt.Errorf("write queue full")
	}
}

// versionToBytes encodes a version counter as 8-byte little-endian (vclock).
func versionToBytes(v int64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(v))
	return buf
}

// bytesToVersion decodes an 8-byte little-endian vclock to a version counter.
func bytesToVersion(b []byte) int64 {
	if len(b) != 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(b))
}

// Connect creates the keyspace and tables, then opens a session.
func (kv *ScyllaKVStore) Connect() error {
	// First connect without keyspace to create it
	cluster := gocql.NewCluster(kv.hosts...)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 10 * time.Second

	initSession, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("failed to connect to ScyllaDB: %w", err)
	}

	err = initSession.Query(fmt.Sprintf(
		`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`,
		kv.keyspace,
	)).Exec()
	initSession.Close()
	if err != nil {
		return fmt.Errorf("failed to create keyspace: %w", err)
	}

	// Reconnect with keyspace and create tables
	cluster = gocql.NewCluster(kv.hosts...)
	cluster.Keyspace = kv.keyspace
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 10 * time.Second

	kv.session, err = cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("failed to connect to keyspace %s: %w", kv.keyspace, err)
	}

	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS metadata (
			key bigint PRIMARY KEY,
			data blob,
			version bigint
		)`,
		`CREATE TABLE IF NOT EXISTS chunks (
			key bigint PRIMARY KEY,
			data blob
		)`,
	} {
		if err := kv.session.Query(stmt).Exec(); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	return nil
}

// InitBackend creates the root inode (inode 0).
func (kv *ScyllaKVStore) InitBackend() error {
	existing, _, err := kv.Metadata(0)
	if err == nil && existing != nil {
		return fmt.Errorf("filesystem already exists (inode 0 found in keyspace %s). Use WipeBackend() first to reinitialize", kv.keyspace)
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
		Chunks:       nil,
		Nlink:        2,
		BlockSize:    GetChunkSize(),
	}

	if _, err := kv.PutMetadata(0, rootDir); err != nil {
		return fmt.Errorf("failed to create root inode: %w", err)
	}
	return nil
}

// Close shuts down the write workers and closes the ScyllaDB session.
func (kv *ScyllaKVStore) Close() error {
	close(kv.writeQueue)
	kv.writeWg.Wait()
	if kv.session != nil {
		kv.session.Close()
	}
	return nil
}

// PutMetadata inserts new metadata with IF NOT EXISTS (optimistic concurrency).
func (kv *ScyllaKVStore) PutMetadata(key uint64, newMeta *bangpb.InodeMeta) ([]byte, error) {
	data, err := proto.Marshal(newMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	var version int64 = 1
	applied, err := kv.session.Query(
		`INSERT INTO metadata (key, data, version) VALUES (?, ?, ?) IF NOT EXISTS`,
		int64(key), data, version,
	).ScanCAS(nil, nil, nil) // discard returned columns
	if err != nil {
		return nil, fmt.Errorf("failed to insert metadata: %w", err)
	}
	if !applied {
		return nil, fmt.Errorf("key already exists: %d", key)
	}

	return versionToBytes(version), nil
}

// Metadata retrieves inode metadata and its version (vclock).
func (kv *ScyllaKVStore) Metadata(key uint64) (*bangpb.InodeMeta, []byte, error) {
	var data []byte
	var version int64

	if err := kv.session.Query(
		`SELECT data, version FROM metadata WHERE key = ?`, int64(key),
	).Scan(&data, &version); err != nil {
		return nil, nil, fmt.Errorf("key not found: %d", key)
	}

	meta := &bangpb.InodeMeta{}
	if err := proto.Unmarshal(data, meta); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return meta, versionToBytes(version), nil
}

// UpdateMetadata updates metadata with IF version = ? (optimistic concurrency).
func (kv *ScyllaKVStore) UpdateMetadata(key uint64, newMeta *bangpb.InodeMeta, vclockIn []byte) ([]byte, error) {
	data, err := proto.Marshal(newMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	oldVersion := bytesToVersion(vclockIn)
	newVersion := oldVersion + 1

	applied, err := kv.session.Query(
		`UPDATE metadata SET data = ?, version = ? WHERE key = ? IF version = ?`,
		data, newVersion, int64(key), oldVersion,
	).ScanCAS(nil, nil) // discard returned columns
	if err != nil {
		return nil, fmt.Errorf("failed to update metadata: %w", err)
	}
	if !applied {
		return nil, fmt.Errorf("concurrent modification on key %d (version mismatch)", key)
	}

	return versionToBytes(newVersion), nil
}

// DeleteMetadata deletes metadata with IF version = ? (optimistic concurrency).
func (kv *ScyllaKVStore) DeleteMetadata(key uint64, vclockIn []byte) error {
	if vclockIn != nil {
		oldVersion := bytesToVersion(vclockIn)
		applied, err := kv.session.Query(
			`DELETE FROM metadata WHERE key = ? IF version = ?`,
			int64(key), oldVersion,
		).ScanCAS(nil) // discard returned columns
		if err != nil {
			return fmt.Errorf("failed to delete metadata: %w", err)
		}
		if !applied {
			return fmt.Errorf("concurrent modification on key %d (version mismatch)", key)
		}
		return nil
	}

	// No vclock — unconditional delete
	if err := kv.session.Query(
		`DELETE FROM metadata WHERE key = ?`, int64(key),
	).Exec(); err != nil {
		return fmt.Errorf("failed to delete metadata: %w", err)
	}
	return nil
}

// PutChunk stores a chunk (async via write queue, cache-through).
func (kv *ScyllaKVStore) PutChunk(key uint64, data []byte) error {
	kv.chunkCache.Add(key, chunkEntry{data: data, dirty: true})
	k := key
	return kv.enqueue(
		`INSERT INTO chunks (key, data) VALUES (?, ?)`,
		[]interface{}{int64(key), data},
		&k,
	)
}

// Chunk retrieves a chunk by key (cache-first).
func (kv *ScyllaKVStore) Chunk(key uint64) ([]byte, error) {
	if entry, ok := kv.chunkCache.Get(key); ok {
		return entry.data, nil
	}

	var data []byte
	if err := kv.session.Query(
		`SELECT data FROM chunks WHERE key = ?`, int64(key),
	).Scan(&data); err != nil {
		return nil, fmt.Errorf("chunk not found: %016x", key)
	}
	kv.chunkCache.Add(key, chunkEntry{data: data, dirty: false})
	return data, nil
}

// DeleteChunk deletes a chunk (async via write queue).
func (kv *ScyllaKVStore) DeleteChunk(key uint64) error {
	kv.chunkCache.Remove(key)
	return kv.enqueue(
		`DELETE FROM chunks WHERE key = ?`,
		[]interface{}{int64(key)},
		nil,
	)
}

// WipeBackend truncates both metadata and chunks tables.
func (kv *ScyllaKVStore) WipeBackend(w io.Writer) error {
	fmt.Fprintf(w, "  wiping metadata table in keyspace %s...\n", kv.keyspace)
	if err := kv.session.Query(`TRUNCATE metadata`).Exec(); err != nil {
		return fmt.Errorf("failed to truncate metadata: %w", err)
	}
	fmt.Fprintf(w, "  metadata wiped\n")

	fmt.Fprintf(w, "  wiping chunks table in keyspace %s...\n", kv.keyspace)
	if err := kv.session.Query(`TRUNCATE chunks`).Exec(); err != nil {
		return fmt.Errorf("failed to truncate chunks: %w", err)
	}
	fmt.Fprintf(w, "  chunks wiped\n")

	return nil
}

// DiskUsage returns estimated disk usage from ScyllaDB system tables.
// This queries the local node's data directory via statfs as a rough estimate.
func (kv *ScyllaKVStore) DiskUsage(chunkSize uint32) (*DiskUsageInfo, error) {
	// ScyllaDB doesn't expose per-keyspace disk usage easily via CQL.
	// As a practical approach, query the data directory size via the OS.
	// Default ScyllaDB data dir is /var/lib/scylla/data.
	dataDir := "/var/lib/scylla/data"

	var stat syscall.Statfs_t
	if err := syscall.Statfs(dataDir, &stat); err != nil {
		return nil, fmt.Errorf("statfs on %s: %w", dataDir, err)
	}

	bsize := uint64(stat.Bsize)
	cs := uint64(chunkSize)
	totalBytes := stat.Blocks * bsize
	usedBytes := (stat.Blocks - stat.Bfree) * bsize

	return &DiskUsageInfo{
		TotalChunks: totalBytes / cs,
		UsedChunks:  usedBytes / cs,
	}, nil
}
