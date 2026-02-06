package fuse

import (
	"fmt"
	"syscall"
	"time"

	riak "github.com/basho/riak-go-client"
	"google.golang.org/protobuf/proto"

	pb "bangfs/proto"
	"bangfs/util"
)

// KVStore holds a connection to the Riak backend
type KVStore struct {
	metadataBucket string
	chunkBucket    string
	cluster        *riak.Cluster
	host           string
	port           uint16
}

// NewKVStore creates a new KVStore instance
func NewKVStore(host string, port uint16, namespace string) (*KVStore, error) {
	kv := &KVStore{
		metadataBucket: namespace + "_bangfs_metadata",
		chunkBucket:    namespace + "_bangfs_chunks",
		host:           host,
		port:           port,
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
	nodeAddr := fmt.Sprintf("%s:%d", kv.host, kv.port)
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

// getVersion extracts the version from metadata
func getVersion(meta *pb.InodeMeta) uint64 {
	switch m := meta.Meta.(type) {
	case *pb.InodeMeta_File:
		return m.File.Version
	case *pb.InodeMeta_Dir:
		return m.Dir.Version
	}
	return 0
}

// setVersion sets the version on metadata
func setVersion(meta *pb.InodeMeta, version uint64) {
	switch m := meta.Meta.(type) {
	case *pb.InodeMeta_File:
		m.File.Version = version
	case *pb.InodeMeta_Dir:
		m.Dir.Version = version
	}
}

// PutMetadata stores inode metadata with optimistic concurrency control.
// If oldMeta is non-nil, it verifies the current version matches before updating.
// The version is automatically incremented on successful write.
func (kv *KVStore) PutMetadata(key uint64, oldMeta, newMeta *pb.InodeMeta) error {
	tracer := util.GetTracer()

	// If oldMeta provided, do compare-and-set
	if oldMeta != nil {
		trace := tracer.KV("PutMetadata.CAS", key)
		currentMeta, err := kv.Metadata(key)
		if err != nil {
			trace.Error(err)
			return fmt.Errorf("failed to fetch current metadata for CAS: %w", err)
		}

		oldVersion := getVersion(oldMeta)
		currentVersion := getVersion(currentMeta)

		if oldVersion != currentVersion {
			err := fmt.Errorf("%w: expected version %d, got %d", ErrConcurrentModification, oldVersion, currentVersion)
			trace.Error(err)
			return err
		}

		// Increment version for the new write
		newVersion := currentVersion + 1
		setVersion(newMeta, newVersion)
		trace.Done()
		tracer.KV(fmt.Sprintf("PutMetadata.version %d->%d", currentVersion, newVersion), key).Done()
	} else {
		// No CAS, but still increment version if it's 0 (new object)
		if getVersion(newMeta) == 0 {
			setVersion(newMeta, 1)
			tracer.KV("PutMetadata.init version=1", key).Done()
		}
	}

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

	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucket(kv.metadataBucket).
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

// GetMeta retrieves inode metadata
func (kv *KVStore) Metadata(key uint64) (*pb.InodeMeta, error) {
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucket(kv.metadataBucket).
		WithKey(fmt.Sprintf("%d", key)).
		Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build fetch command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return nil, fmt.Errorf("failed to execute fetch: %w", err)
	}

	fvc := cmd.(*riak.FetchValueCommand)
	if fvc.Response == nil || len(fvc.Response.Values) == 0 {
		return nil, fmt.Errorf("key not found: %d", key)
	}

	meta := &pb.InodeMeta{}
	if err := proto.Unmarshal(fvc.Response.Values[0].Value, meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}
	return meta, nil
}

// DeleteMeta deletes inode metadata
func (kv *KVStore) DeleteMetadata(key uint64) error {
	cmd, err := riak.NewDeleteValueCommandBuilder().
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

// AllocInode allocates a new unique inode number
// Uses key "next_inode" in metadata bucket to track the counter
func (kv *KVStore) AllocInode() (uint64, error) {
	const counterKey = "next_inode"

	// Fetch current counter
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucket(kv.metadataBucket).
		WithKey(counterKey).
		Build()
	if err != nil {
		return 0, fmt.Errorf("failed to build fetch command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return 0, fmt.Errorf("failed to fetch counter: %w", err)
	}

	var nextInode uint64 = 1 // Start at 1 (0 is root)
	fvc := cmd.(*riak.FetchValueCommand)
	if fvc.Response != nil && len(fvc.Response.Values) > 0 {
		// Parse existing counter
		fmt.Sscanf(string(fvc.Response.Values[0].Value), "%d", &nextInode)
	}

	// Increment and store
	newCounter := nextInode + 1
	obj := &riak.Object{
		Bucket:      kv.metadataBucket,
		Key:         counterKey,
		ContentType: "text/plain",
		Value:       []byte(fmt.Sprintf("%d", newCounter)),
	}

	storeCmd, err := riak.NewStoreValueCommandBuilder().
		WithBucket(kv.metadataBucket).
		WithContent(obj).
		Build()
	if err != nil {
		return 0, fmt.Errorf("failed to build store command: %w", err)
	}

	if err := kv.cluster.Execute(storeCmd); err != nil {
		return 0, fmt.Errorf("failed to store counter: %w", err)
	}

	return nextInode, nil
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
	if err := kv.wipeBucket(kv.metadataBucket); err != nil {
		return fmt.Errorf("failed to wipe metadata bucket: %w", err)
	}

	// Delete all chunks
	if err := kv.wipeBucket(kv.chunkBucket); err != nil {
		return fmt.Errorf("failed to wipe chunk bucket: %w", err)
	}

	return nil
}

// wipeBucket deletes all keys in a bucket
func (kv *KVStore) wipeBucket(bucket string) error {
	// List all keys in the bucket
	cmd, err := riak.NewListKeysCommandBuilder().
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

Then use buckets:
  metadata: %s (metadata)
  chunks:   %s (chunks)
  host:     %s:%d`,
		kv.metadataBucket, kv.metadataBucket,
		kv.chunkBucket, kv.chunkBucket,

		kv.metadataBucket,
		kv.chunkBucket,
		kv.host, kv.port)
}

// InitBackend initializes backend buckets with appropriate settings
// DESIGN DECISION for reliability (tradeoffs):
// - chunk bucket: if N=4, then R=1, W=3 (fast reads, durable writes)
// - metadata bucket: strong consistency enabled
//
// Note: Bucket properties should be configured via Riak admin bucket types:
//
//	# Metadata bucket: strong consistency enabled
//	riak-admin bucket-type create bangfs_meta '{"props":{"consistent":true}}'
//	riak-admin bucket-type activate bangfs_meta
//
//	# Chunk bucket: eventual consistency, optimized for reads
//	riak-admin bucket-type create bangfs_chunks '{"props":{"n_val":4,"w":3,"r":1}}'
//	riak-admin bucket-type activate bangfs_chunks
func (kv *KVStore) InitBackend() error {
	if kv.cluster == nil {
		return fmt.Errorf("cluster not connected to %s:%d", kv.host, kv.port)
	}

	// Check if filesystem already exists (inode 0 present)
	existing, err := kv.Metadata(0)
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

	if err := kv.PutMetadata(0, nil, rootDir); err != nil {
		return fmt.Errorf("failed to create root inode: %w\n\n%s", err, kv.SetupInstructions())
	}

	return nil
}
