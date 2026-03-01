package bangfuse

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"syscall"
	"time"

	riak "github.com/basho/riak-go-client"
	"google.golang.org/protobuf/proto"

	bangpb "bangfs/proto"

	"bangfs/bangutil"
)

const metadataBucket = "metadata"
const chunkBucket = "chunks"

const statsHTTPTimeout = 5 * time.Second

// RiakKVStore holds a connection to the Riak backend
type RiakKVStore struct {
	metadataBucketType string
	chunkBucketType    string
	cluster            *riak.Cluster
	host               string
	pb_port            uint16
	httpPort           uint16 // Riak HTTP API port for stats (default 8098)
	dataPath           string // preferred disk mount point for df (default "/data")
}

// NewRiakKVStore creates a new KVStore instance.
// httpPort and dataPath are used for DiskUsage (df support).
// Pass 0 and "" respectively for defaults.
func NewRiakKVStore(host string, port uint16, namespace string, httpPort uint16, dataPath string) (*RiakKVStore, error) {
	if httpPort == 0 {
		httpPort = 8098
	}
	if dataPath == "" {
		dataPath = "/data"
	}
	kv := &RiakKVStore{
		metadataBucketType: namespace + "_bangfs_metadata",
		chunkBucketType:    namespace + "_bangfs_chunks",
		host:               host,
		pb_port:            port,
		httpPort:           httpPort,
		dataPath:           dataPath,
	}
	if err := kv.Connect(); err != nil {
		return kv, err // for latter printing of the values
	}
	return kv, nil
}

// Connect connects or reconnects to the backend
func (kv *RiakKVStore) Connect() error {
	node_addr := fmt.Sprintf("%s:%d", kv.host, kv.pb_port)
	node_opts := &riak.NodeOptions{
		RemoteAddress: node_addr,
		/* ExecutionAttempts      byte
		QueueMaxDepth          uint16
		QueueExecutionInterval time.Duration
		*/
	}
	node, err := riak.NewNode(node_opts)
	if err != nil {
		return fmt.Errorf("failed to create node: %w", err)
	}

	cluster_opts := &riak.ClusterOptions{
		Nodes: []*riak.Node{node},
	}
	cluster, err := riak.NewCluster(cluster_opts)
	if err != nil {
		return fmt.Errorf("failed to create cluster: %w", err)
	}

	if err := cluster.Start(); err != nil {
		return fmt.Errorf("failed to start cluster: %w", err)
	}

	kv.cluster = cluster
	return nil
}

// InitBackend initializes backend root node.
// thus making a new filesystem in the namespace.
func (kv *RiakKVStore) InitBackend() error {
	if kv.cluster == nil {
		return fmt.Errorf("cluster not connected to %s:%d", kv.host, kv.pb_port)
	}

	// Check if filesystem already exists (inode 0 present)
	existing, _, err := kv.Metadata(0)
	if err == nil && existing != nil {
		return fmt.Errorf("filesystem already exists (inode 0 found in bucket %s). Use WipeBackend() first to reinitialize", kv.metadataBucketType)
	}

	// Create root inode (inode 0) as empty directory
	now := time.Now().UnixNano()
	root_dir := &bangpb.InodeMeta{
		Name: "", ParentInode: 0, // root is its own parent // TODO: leave out for directory?
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

	if _, err := kv.PutMetadata(0, root_dir); err != nil {
		return fmt.Errorf("failed to create root inode: %w\n", err)
	}

	return nil
}

// Close closes the connection to the backend
func (kv *RiakKVStore) Close() error {
	if kv.cluster != nil {
		return kv.cluster.Stop()
	}
	return nil
}

// PutMetadata creates new metadata entries with optimistic concurrency control:
// If the key already exists, the function will fail.
// TODO: implement retries
func (kv *RiakKVStore) PutMetadata(key uint64, newMeta *bangpb.InodeMeta) ([]byte, error) {

	data, err := proto.Marshal(newMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	obj := &riak.Object{
		Bucket:      metadataBucket,
		BucketType:  kv.metadataBucketType, // TODO: Check if passing ButcketType is redundant
		Key:         fmt.Sprintf("%d", key),
		ContentType: "application/protobuf",
		Value:       data,
	}

	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucketType(kv.metadataBucketType).
		WithBucket(metadataBucket).
		WithContent(obj).
		WithIfNoneMatch(true). // Concurrency control here!
		WithReturnBody(true).  // Need this to get vclock back
		Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build store command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return nil, fmt.Errorf("failed to execute store: %w", err)
	}

	svc := cmd.(*riak.StoreValueCommand)
	vclock := svc.Response.VClock
	if len(vclock) == 0 {
		return nil, fmt.Errorf("didn't get vclock")
	}
	return vclock, nil
}

// UpdateMetadata stores inode metadata with optimistic concurrency control.
// Its intended to fail if the metadata has been updated since the last read.
func (kv *RiakKVStore) UpdateMetadata(key uint64, newMeta *bangpb.InodeMeta, vclock_in []byte) ([]byte, error) {

	data, err := proto.Marshal(newMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	obj := &riak.Object{
		Bucket:      metadataBucket,
		BucketType:  kv.metadataBucketType,
		Key:         fmt.Sprintf("%d", key),
		ContentType: "application/protobuf",
		Value:       data,
		VClock:      vclock_in,
	}

	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucketType(kv.metadataBucketType).
		WithBucket(metadataBucket).
		WithContent(obj).
		WithIfNotModified(true). // Concurrency control here!
		WithReturnBody(true).    // Need this to get vclock back
		Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build store command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return nil, fmt.Errorf("failed to execute store: %w", err)
	}
	svc := cmd.(*riak.StoreValueCommand)
	return svc.Response.VClock, err
}

// GetMeta retrieves inode metadata
func (kv *RiakKVStore) Metadata(key uint64) (*bangpb.InodeMeta /*vclock*/, []byte, error) {
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucketType(kv.metadataBucketType).
		WithBucket(metadataBucket).
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

	meta := &bangpb.InodeMeta{}
	if err := proto.Unmarshal(fvc.Response.Values[0].Value, meta); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}
	return meta, fvc.Response.VClock, nil
}

// DeleteMetadata deletes inode metadata with optimistic concurrency control.
// Pass the vclock from the last read to ensure no concurrent modification.
func (kv *RiakKVStore) DeleteMetadata(key uint64, vclockIn []byte) error {
	builder := riak.NewDeleteValueCommandBuilder().
		WithBucketType(kv.metadataBucketType).
		WithBucket(metadataBucket).
		WithKey(fmt.Sprintf("%d", key))
	if vclockIn != nil {
		builder = builder.WithVClock(vclockIn)
	}
	cmd, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed to build delete command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return fmt.Errorf("failed to execute delete: %w", err)
	}
	return nil
}

// PutChunk stores a chunk by its key
func (kv *RiakKVStore) PutChunk(key uint64, data []byte) error {
	obj := &riak.Object{
		Bucket:      chunkBucket,
		BucketType:  kv.chunkBucketType,
		Key:         fmt.Sprintf("%016x", key),
		ContentType: "application/octet-stream",
		Value:       data,
	}

	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucketType(kv.chunkBucketType).
		WithBucket(chunkBucket).
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

// Chunk retrieves a chunk by its key
func (kv *RiakKVStore) Chunk(key uint64) ([]byte, error) {
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucketType(kv.chunkBucketType).
		WithBucket(chunkBucket).
		WithKey(fmt.Sprintf("%016x", key)).
		Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build fetch command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return nil, fmt.Errorf("failed to execute fetch: %w", err)
	}

	fvc := cmd.(*riak.FetchValueCommand)
	if fvc.Response == nil || len(fvc.Response.Values) == 0 {
		return nil, fmt.Errorf("chunk not found: %016x", key)
	}

	return fvc.Response.Values[0].Value, nil
}

// DeleteChunk deletes a chunk by its key
func (kv *RiakKVStore) DeleteChunk(key uint64) error {
	cmd, err := riak.NewDeleteValueCommandBuilder().
		WithBucketType(kv.chunkBucketType).
		WithBucket(chunkBucket).
		WithKey(fmt.Sprintf("%016x", key)).
		Build()
	if err != nil {
		return fmt.Errorf("failed to build delete command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return fmt.Errorf("failed to execute delete: %w", err)
	}
	return nil
}

// WipeBackend deletes all metadata and chunks from the backend.
// Progress is written to w (pass io.Discard or os.Stderr).
func (kv *RiakKVStore) WipeBackend(w io.Writer) error {
	if kv.cluster == nil {
		return fmt.Errorf("cluster not connected")
	}

	fmt.Fprintf(w, "  wiping metadata [%s/%s]...\n", kv.metadataBucketType, metadataBucket)
	num_meta_keys, err := kv.wipeBucket(w, kv.metadataBucketType, metadataBucket)
	if err != nil {
		return fmt.Errorf("failed to wipe metadata bucket: %w", err)
	}
	fmt.Fprintf(w, "  deleted %d metadata keys\n", num_meta_keys)

	fmt.Fprintf(w, "  wiping chunks [%s/%s]...\n", kv.chunkBucketType, chunkBucket)
	num_chunk_keys, err := kv.wipeBucket(w, kv.chunkBucketType, chunkBucket)
	if err != nil {
		return fmt.Errorf("failed to wipe chunk bucket: %w", err)
	}
	fmt.Fprintf(w, "  deleted %d chunk keys\n", num_chunk_keys)

	return nil
}

// wipeBucket deletes all keys in a bucket
func (kv *RiakKVStore) wipeBucket(w io.Writer, bucketType, bucket string) (int, error) {

	// List all keys in the bucket
	cmd, err := riak.NewListKeysCommandBuilder().
		WithBucketType(bucketType).
		WithBucket(bucket).
		WithStreaming(false).
		Build()
	if err != nil {
		return 0, fmt.Errorf("failed to build list keys command: %w", err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return 0, fmt.Errorf("failed to list keys: %w", err)
	}

	lkc := cmd.(*riak.ListKeysCommand)
	if lkc.Response == nil {
		return 0, fmt.Errorf("no keys found in bucket: %v", bucket)
	}

	total := len(lkc.Response.Keys)
	fmt.Fprintf(w, "  found %d keys in %s/%s\n", total, bucketType, bucket)

	// Delete each key (fetch VClock first so Riak properly resolves the delete)
	keycount := 0
	for _, key := range lkc.Response.Keys {
		// Fetch the object to get its VClock
		fetch_cmd, err := riak.NewFetchValueCommandBuilder().
			WithBucketType(bucketType).
			WithBucket(bucket).
			WithKey(string(key)).
			Build()
		if err != nil {
			return 0, fmt.Errorf("failed to build fetch command for key %s: %w", key, err)
		}
		if err := kv.cluster.Execute(fetch_cmd); err != nil {
			return 0, fmt.Errorf("failed to fetch key %s for vclock: %w", key, err)
		}

		del_builder := riak.NewDeleteValueCommandBuilder().
			WithBucketType(bucketType).
			WithBucket(bucket).
			WithKey(string(key))

		fvc := fetch_cmd.(*riak.FetchValueCommand)
		if fvc.Response != nil && len(fvc.Response.VClock) > 0 {
			del_builder = del_builder.WithVClock(fvc.Response.VClock)
		}

		del_cmd, err := del_builder.Build()
		if err != nil {
			return 0, fmt.Errorf("failed to build delete command for key %s: %w", key, err)
		}

		if err := kv.cluster.Execute(del_cmd); err != nil {
			return 0, fmt.Errorf("failed to delete key %s: %w", key, err)
		}
		keycount++
	}

	return keycount, nil
}

// --- DiskUsage: Riak HTTP stats ---
// Block usage / free is synthesized from free disk space on each cluster member (bytes/chunksize)
// Its not intended to be accurate but rather a rough estimate of how much space is available for new data.
//

// riakStatsResponse holds the fields we care about from GET /stats
type riakStatsResponse struct {
	RingMembers []string        `json:"ring_members"`
	Disk        []riakDiskEntry `json:"disk"`
}

// riakDiskEntry represents a single disk entry from Riak's /stats endpoint.
type riakDiskEntry struct {
	ID   string `json:"id"`
	Size uint64 `json:"size"`
	Used uint64 `json:"used"`
}

// DiskUsage returns aggregate disk usage across all Riak cluster nodes.
func (kv *RiakKVStore) DiskUsage(chunkSize uint32) (*DiskUsageInfo, error) {
	client := &http.Client{Timeout: statsHTTPTimeout}
	op := bangutil.GetTracer().Op("DiskUsage", 0, "")

	// Get ring_members from the seed host
	seedURL := fmt.Sprintf("http://%s:%d/stats", kv.host, kv.httpPort)
	seedStats, err := fetchRiakStats(client, seedURL)
	if err != nil {
		op.Error(err)
		return nil, fmt.Errorf("failed to fetch seed stats from %s: %w", seedURL, err)
	}

	// Extract host addresses from ring_members (e.g. "riak@192.168.1.1" -> "192.168.1.1")
	hosts := extractHostsFromMembers(seedStats.RingMembers)
	if len(hosts) == 0 {
		hosts = []string{kv.host} // fallback to seed host
	}

	// Query each host and sum disk usage
	var totalBytes, usedBytes uint64
	respondedCount := 0
	cs := uint64(chunkSize)

	for _, h := range hosts {
		url := fmt.Sprintf("http://%s:%d/stats", h, kv.httpPort)
		stats, err := fetchRiakStats(client, url)
		if err != nil {
			op.Errorf("skipping host %s: %v", h, err)
			continue
		}
		total, used, err := extractDiskInfo(stats, kv.dataPath)
		if err != nil {
			op.Errorf("skipping host %s disk info: %v", h, err)
			continue
		}
		totalBytes += total
		usedBytes += used
		respondedCount++
	}

	if respondedCount == 0 {
		return nil, fmt.Errorf("no Riak hosts responded with disk info")
	}

	return &DiskUsageInfo{
		TotalChunks: totalBytes / cs,
		UsedChunks:  usedBytes / cs,
	}, nil
}

// fetchRiakStats fetches and parses JSON from a Riak /stats endpoint.
func fetchRiakStats(client *http.Client, url string) (*riakStatsResponse, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}
	var stats riakStatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, fmt.Errorf("failed to decode stats: %w", err)
	}
	return &stats, nil
}

// extractHostsFromMembers parses Riak ring_members like "riak@192.168.1.1"
// into host addresses.
func extractHostsFromMembers(members []string) []string {
	var hosts []string
	seen := map[string]bool{}
	for _, m := range members {
		parts := strings.SplitN(m, "@", 2)
		if len(parts) == 2 {
			h := parts[1]
			if !seen[h] {
				hosts = append(hosts, h)
				seen[h] = true
			}
		}
	}
	return hosts
}

// extractDiskInfo picks the best disk entry from the stats response.
// Priority: exact match on preferredPath, then any path containing "data", then "/".
// Returns totalBytes, usedBytes.
func extractDiskInfo(stats *riakStatsResponse, preferredPath string) (uint64, uint64, error) {
	if len(stats.Disk) == 0 {
		return 0, 0, fmt.Errorf("no disk data in stats response")
	}

	var best *riakDiskEntry
	for i := range stats.Disk {
		e := &stats.Disk[i]
		if e.ID == preferredPath {
			best = e
			break
		}
	}
	if best == nil {
		for i := range stats.Disk {
			e := &stats.Disk[i]
			if strings.Contains(e.ID, "data") {
				best = e
				break
			}
		}
	}
	if best == nil {
		for i := range stats.Disk {
			e := &stats.Disk[i]
			if e.ID == "/" {
				best = e
				break
			}
		}
	}
	if best == nil {
		best = &stats.Disk[0] // last resort: first entry
	}

	return best.Size, best.Used, nil
}
