package fuse

import (
	"fmt"
	"syscall"
	"time"

	riak "github.com/basho/riak-go-client"
	"google.golang.org/protobuf/proto"

	pb "bangfs/proto"
	//"bangfs/util"
)

// KVStore holds a connection to the Riak backend
type KVStore struct {
	metadataBucket string
	chunkBucket    string
	counterBucket  string
	cluster        *riak.Cluster
	host           string
	pb_port        uint16
	//rest_port      uint16
}

// NewKVStore creates a new KVStore instance
func NewKVStore(host string, port uint16, namespace string) (*KVStore, error) {
	kv := &KVStore{
		metadataBucket: namespace + "_bangfs_metadata",
		chunkBucket:    namespace + "_bangfs_chunks",
		counterBucket:  namespace + "_bangfs_counters",
		host:           host,
		pb_port:        port,
		//rest_port:      8098, // TODO: default, add option later.
	}
	if err := kv.Connect(); err != nil {
		return kv, err // use kv for latter printing of the values
	}
	return kv, nil
}

// Connection / network management
// ===============================

// Connect connects or reconnects to the backend
func (kv *KVStore) Connect() error {
	nodeAddr := fmt.Sprintf("%s:%d", kv.host, kv.pb_port)
	nodeOpts := &riak.NodeOptions{
		RemoteAddress: nodeAddr,
	}
	node, err := riak.NewNode(nodeOpts)
	if err != nil {
		return fmt.Errorf("failed to create node: %w", err)
	}

	clusterOpts := &riak.ClusterOptions{
		Nodes: []*riak.Node{node},
	}
	cluster, err := riak.NewCluster(clusterOpts)
	if err != nil {
		return fmt.Errorf("failed to create cluster: %w", err)
	}

	if err := cluster.Start(); err != nil {
		return fmt.Errorf("failed to start cluster: %w", err)
	}

	kv.cluster = cluster
	return nil
}

// Close closes the connection to the backend
func (kv *KVStore) Close() error {
	if kv.cluster != nil {
		return kv.cluster.Stop()
	}
	return nil
}

// CRUD ops for Metadata
// =====================

// ErrConcurrentModification is returned when a CAS operation fails due to concurrent modification
var ErrConcurrentModification = fmt.Errorf("concurrent modification detected")

// UpdateMetadata stores inode metadata with optimistic concurrency control.
// If oldMeta is non-nil, it verifies the current version matches before updating.
// The version is automatically incremented on successful write.
func (kv *KVStore) PutMetadata(key uint64, newMeta *pb.InodeMeta) error {
	//tracer := util.GetTracer()

	data, err := proto.Marshal(newMeta)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	obj := &riak.Object{
		Bucket:      kv.metadataBucket,
		Key:         fmt.Sprintf("%d", key),
		ContentType: "application/protobuf",
		Value:       data,
	}

	// Use WithIfNotModified to prevent inconsistent writes.
	// Its probably overkill for some attributes (permission changes, etc)
	// but its convenient and will prevent race conditions.
	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucketType(kv.metadataBucket).
		WithBucket(kv.metadataBucket).
		WithContent(obj).
		WithIfNoneMatch(true).
		Build()
	if err != nil {
		return fmt.Errorf("failed to build store command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return fmt.Errorf("failed to execute store: %w", err)
	}
	return nil
}
// UpdateMetadata stores inode metadata with optimistic concurrency control.
// If oldMeta is non-nil, it verifies the current version matches before updating.
// The version is automatically incremented on successful write.
func (kv *KVStore) UpdateMetadata(key uint64, newMeta *pb.InodeMeta, vclock_in []byte) error {
	//tracer := util.GetTracer()

	data, err := proto.Marshal(newMeta)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	obj := &riak.Object{
		Bucket:      kv.metadataBucket,
		Key:         fmt.Sprintf("%d", key),
		ContentType: "application/protobuf",
		Value:       data,
		VClock:      vclock_in,
	}

	// Use WithIfNotModified to prevent inconsistent writes.
	// Its probably overkill for some attributes (permission changes, etc)
	// but its convenient and will prevent race conditions.
	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucketType(kv.metadataBucket).
		WithBucket(kv.metadataBucket).
		WithContent(obj).
		WithIfNotModified(true).
		Build()
	if err != nil {
		return fmt.Errorf("failed to build store command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return fmt.Errorf("failed to execute store: %w", err)
	}
	return nil
}

// GetMeta retrieves inode metadata
func (kv *KVStore) Metadata(key uint64) (*pb.InodeMeta, /*vclock*/ []byte, error) {
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucketType(kv.metadataBucket).
		WithBucket(kv.metadataBucket).
		WithKey(fmt.Sprintf("%d", key)).
		Build()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build fetch command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return nil, nil, fmt.Errorf("failed to execute fetch: %w", err)
	}

	fvc := cmd.(*riak.FetchValueCommand)
	if fvc.Response == nil || len(fvc.Response.Values) == 0 {
		return nil, nil, fmt.Errorf("key not found: %d", key)
	}

	meta := &pb.InodeMeta{}
	if err := proto.Unmarshal(fvc.Response.Values[0].Value, meta); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}
	return meta, fvc.Response.VClock, nil
}

// DeleteMeta deletes inode metadata
func (kv *KVStore) DeleteMetadata(key uint64) error {
	cmd, err := riak.NewDeleteValueCommandBuilder().
		WithBucketType(kv.metadataBucket).
		WithBucket(kv.metadataBucket).
		WithKey(fmt.Sprintf("%d", key)).
		Build()
	if err != nil {
		return fmt.Errorf("failed to build delete command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return fmt.Errorf("failed to execute delete: %w", err)
	}
	return nil
}

// AllocInode allocates a new unique inode number using a Riak CRDT counter.
// The counter is atomic so concurrent allocations are safe.
func (kv *KVStore) AllocInode() (uint64, error) {
	cmd, err := riak.NewUpdateCounterCommandBuilder().
		WithBucketType(kv.counterBucket).
		WithBucket(kv.counterBucket).
		WithKey("next_inode").
		WithIncrement(1).
		WithReturnBody(true).
		Build()
	if err != nil {
		return 0, fmt.Errorf("failed to build counter command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return 0, fmt.Errorf("failed to increment counter: %w", err)
	}

	ucc := cmd.(*riak.UpdateCounterCommand)
	if ucc.Response == nil {
		return 0, fmt.Errorf("no response from counter update")
	}

	// Counter starts at 0, first increment returns 1.
	// Inode 0 is root (created by InitBackend), so first alloc returns 1.
	return uint64(ucc.Response.CounterValue), nil
}

// CRUD ops for Chunks
// ===================

// PutChunk stores a chunk by its FNV hash
func (kv *KVStore) PutChunk(hash []byte, data []byte) error {
	obj := &riak.Object{
		Bucket:      kv.chunkBucket,
		Key:         fmt.Sprintf("%x", hash),
		ContentType: "application/octet-stream",
		Value:       data,
	}

	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucketType(kv.chunkBucket).
		WithBucket(kv.chunkBucket).
		WithContent(obj).
		Build()
	if err != nil {
		return fmt.Errorf("failed to build store command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return fmt.Errorf("failed to execute store: %w", err)
	}
	return nil
}

// GetChunk retrieves a chunk by its FNV hash
func (kv *KVStore) GetChunk(hash []byte) ([]byte, error) {
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucketType(kv.chunkBucket).
		WithBucket(kv.chunkBucket).
		WithKey(fmt.Sprintf("%x", hash)).
		Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build fetch command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return nil, fmt.Errorf("failed to execute fetch: %w", err)
	}

	fvc := cmd.(*riak.FetchValueCommand)
	if fvc.Response == nil || len(fvc.Response.Values) == 0 {
		return nil, fmt.Errorf("chunk not found: %x", hash)
	}

	return fvc.Response.Values[0].Value, nil
}

// DeleteChunk deletes a chunk by its FNV hash
func (kv *KVStore) DeleteChunk(hash []byte) error {
	cmd, err := riak.NewDeleteValueCommandBuilder().
		WithBucketType(kv.chunkBucket).
		WithBucket(kv.chunkBucket).
		WithKey(fmt.Sprintf("%x", hash)).
		Build()
	if err != nil {
		return fmt.Errorf("failed to build delete command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return fmt.Errorf("failed to execute delete: %w", err)
	}
	return nil
}

// Initialization
// ==============

// WipeBackend deletes all metadata and chunks - USE WITH CAUTION
// This is a destructive operation that cannot be undone!
func (kv *KVStore) WipeBackend() error {
	if kv.cluster == nil {
		return fmt.Errorf("cluster not connected")
	}

	// Delete all metadata
	if err := kv.wipeBucket(kv.metadataBucket, kv.metadataBucket); err != nil {
		return fmt.Errorf("failed to wipe metadata bucket: %w", err)
	}

	// Delete all chunks
	if err := kv.wipeBucket(kv.chunkBucket, kv.chunkBucket); err != nil {
		return fmt.Errorf("failed to wipe chunk bucket: %w", err)
	}

	return nil
}

// wipeBucket deletes all keys in a bucket
func (kv *KVStore) wipeBucket(bucketType, bucket string) error {
	// List all keys in the bucket
	cmd, err := riak.NewListKeysCommandBuilder().
		WithBucketType(bucketType).
		WithBucket(bucket).
		WithStreaming(false).
		Build()
	if err != nil {
		return fmt.Errorf("failed to build list keys command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return fmt.Errorf("failed to list keys: %w", err)
	}

	lkc := cmd.(*riak.ListKeysCommand)
	if lkc.Response == nil {
		return nil // empty bucket
	}

	// Delete each key
	for _, key := range lkc.Response.Keys {
		delCmd, err := riak.NewDeleteValueCommandBuilder().
			WithBucketType(bucketType).
			WithBucket(bucket).
			WithKey(string(key)).
			Build()
		if err != nil {
			return fmt.Errorf("failed to build delete command for key %s: %w", key, err)
		}

		if err := kv.cluster.Execute(delCmd); err != nil {
			return fmt.Errorf("failed to delete key %s: %w", key, err)
		}
	}

	return nil
}

// SetupInstructions returns setup instructions without requiring a KVStore instance
func (kv *KVStore) SetupInstructions() string {
	return fmt.Sprintf(`Run these commands on a Riak node:

# Metadata bucket: strong consistency enabled
riak-admin bucket-type create %s '{"props":{"consistent":true}}'
riak-admin bucket-type activate %s

# Chunk bucket: eventual consistency, optimized for reads
riak-admin bucket-type create %s '{"props":{"n_val":4,"w":3,"r":1}}'
riak-admin bucket-type activate %s

# Counter bucket: CRDT counter for inode allocation
riak-admin bucket-type create %s '{"props":{"datatype":"counter"}}'
riak-admin bucket-type activate %s

Then use buckets:
  metadata: %s (metadata)
  chunks:   %s (chunks)
  counters: %s (inode allocation)
  host:     %s:%d`,
		kv.metadataBucket, kv.metadataBucket,
		kv.chunkBucket, kv.chunkBucket,
		kv.counterBucket, kv.counterBucket,

		kv.metadataBucket,
		kv.chunkBucket,
		kv.counterBucket,
		kv.host, kv.pb_port)
}

// InitBackend initializes backend buckets with appropriate settings
// DESIGN DECISION for reliability (tradeoffs):
// - chunk bucket: if N=4, then R=1, W=3 (fast reads, durable writes)
// - metadata bucket: strong consistency enabled
func (kv *KVStore) InitBackend() error {
	if kv.cluster == nil {
		return fmt.Errorf("cluster not connected to %s:%d", kv.host, kv.pb_port)
	}

	// Check if filesystem already exists (inode 0 present)
	existing, _, err := kv.Metadata(0)
	if err == nil && existing != nil {
		return fmt.Errorf("filesystem already exists (inode 0 found in bucket %s). Use WipeBackend() first to reinitialize", kv.metadataBucket)
	}

	// Create root inode (inode 0) as empty directory
	now := time.Now().UnixNano()
	rootDir := &pb.InodeMeta{
		Meta: &pb.InodeMeta_Dir{
			Dir: &pb.DirMeta{
				// inode is 0 (the KV key), not stored in proto
				Name:        "",
				ParentInode: 0, // root is its own parent
				Mode:        0755 | syscall.S_IFDIR,
				Uid:         0,
				Gid:         0,
				CtimeNs:     now,
				MtimeNs:     now,
				AtimeNs:     now,
				Children:    []uint64{},
				Nlink:       2,
				Version:     1,
			},
		},
	}

	if err := kv.PutMetadata(0, rootDir); err != nil {
		return fmt.Errorf("failed to create root inode: %w\n\n%s", err, kv.SetupInstructions())
	}

	return nil
}
