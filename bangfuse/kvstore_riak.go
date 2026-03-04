package bangfuse

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"

	riak "github.com/basho/riak-go-client"
	lru "github.com/hashicorp/golang-lru/v2"
	"google.golang.org/protobuf/proto"

	bangpb "bangfs/proto"

	"bangfs/bangutil"
)

const metadataBucket = "metadata"
const chunkBucket = "chunks"

const statsHTTPTimeout = 5 * time.Second

const maxQueueLen = 1000
const numWriteWorkers = 2

type chunkEntry struct {
	data  []byte
	dirty bool
}

// RiakKVStore implements KVStore using Riak KV as the backend.
type RiakKVStore struct {
	metadataBucketType string
	chunkBucketType    string
	cluster            *riak.Cluster
	host               string
	pb_port            uint16
	httpPort           uint16 // Riak HTTP API port for stats (default 8098)
	dataPath           string // preferred disk mount point for df (default "/data")
	chunkCache         *lru.Cache[uint64, chunkEntry]
	writeQueue         chan riak.Command
	writeWg            sync.WaitGroup
	errChan            chan error
}

func (kv *RiakKVStore) dirty(chunkkey uint64) bool {
	if entry, ok := kv.chunkCache.Peek(chunkkey); ok {
		return entry.dirty
	}
	return false
}

func (kv *RiakKVStore) markDirty(chunkkey uint64) {
	if entry, ok := kv.chunkCache.Peek(chunkkey); ok {
		entry.dirty = true
		kv.chunkCache.Add(chunkkey, entry)
	}
}

func (kv *RiakKVStore) markClean(chunkkey uint64) {
	if entry, ok := kv.chunkCache.Peek(chunkkey); ok {
		entry.dirty = false
		kv.chunkCache.Add(chunkkey, entry)
	}
}

// NewRiakKVStore creates a new KVStore instance.
// httpPort and dataPath are used for DiskUsage (df support).
// Pass 0 and "" respectively for defaults.
func NewRiakKVStore(host string, port uint16, namespace string, httpPort uint16, dataPath string, cacheSize int, errorWriter io.Writer) (*RiakKVStore, error) {
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
		writeQueue:         make(chan riak.Command, maxQueueLen),
		errChan:            make(chan error),
	}
	if err := kv.Connect(); err != nil {
		return kv, err // return partially-initialized kv so caller can log host/port
	}

	//chunkCache, cacherr := lru.New[uint64, chunkEntry](max(1, cacheSize/int(GetChunkSize())))
	chunkCache, cacherr := lru.NewWithEvict[uint64, chunkEntry](max(1, cacheSize/int(GetChunkSize())), kv.onEvict)
	if cacherr != nil {
		return kv, cacherr
	}
	kv.chunkCache = chunkCache

	for i := 0; i < numWriteWorkers; i++ {
		clustercon, err := kv.clusterConnect()
		if err != nil {
			return kv, fmt.Errorf("NewRiakKVStore: create worker %d: %v", i, err)
		}
		go kv.riakWriteWorker(clustercon)
	}

	go func() {
		for e := range kv.errChan {
			fmt.Fprintf(errorWriter, "RiakKVStore: %v", e)
		}
	}()

	return kv, nil
}

func (kv *RiakKVStore) clusterConnect() (*riak.Cluster, error) {
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
		return nil, fmt.Errorf("Connect: create node %s: %w", node_addr, err)
	}

	cluster_opts := &riak.ClusterOptions{
		Nodes: []*riak.Node{node},
	}
	cluster, err := riak.NewCluster(cluster_opts)
	if err != nil {
		return nil, fmt.Errorf("Connect: create cluster: %w", err)
	}

	if err := cluster.Start(); err != nil {
		return nil, fmt.Errorf("Connect: start cluster: %w", err)
	}
	return cluster, nil
}

// Connect establishes a connection to the Riak cluster.
func (kv *RiakKVStore) Connect() (err error) {
	kv.cluster, err = kv.clusterConnect()
	return
}

// InitBackend creates the root inode (inode 0), initializing a new filesystem in the namespace.
func (kv *RiakKVStore) InitBackend() error {
	if kv.cluster == nil {
		return fmt.Errorf("InitBackend: cluster not connected to %s:%d", kv.host, kv.pb_port)
	}

	existing, _, err := kv.Metadata(0)
	if err == nil && existing != nil {
		return fmt.Errorf("InitBackend: filesystem already exists (inode 0 in bucket %s), wipe first", kv.metadataBucketType)
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
		return fmt.Errorf("InitBackend: create root inode: %w", err)
	}

	return nil
}

// Close drains the write queue, waits for workers to finish, and stops the cluster.
func (kv *RiakKVStore) Close() error {
	close(kv.writeQueue)
	kv.writeWg.Wait()
	close(kv.errChan)
	if kv.cluster != nil {
		return kv.cluster.Stop()
	}
	return nil
}

// PutMetadata inserts new metadata. Fails if the key already exists (IfNoneMatch).
// Returns the vclock for subsequent updates.
func (kv *RiakKVStore) PutMetadata(key uint64, newMeta *bangpb.InodeMeta) ([]byte, error) {
	data, err := proto.Marshal(newMeta)
	if err != nil {
		return nil, fmt.Errorf("PutMetadata: marshal inode %d: %w", key, err)
	}

	obj := &riak.Object{
		Bucket:      metadataBucket,
		BucketType:  kv.metadataBucketType,
		Key:         fmt.Sprintf("%d", key),
		ContentType: "application/protobuf",
		Value:       data,
	}

	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucketType(kv.metadataBucketType).
		WithBucket(metadataBucket).
		WithContent(obj).
		WithIfNoneMatch(true).
		WithReturnBody(true).
		Build()
	if err != nil {
		return nil, fmt.Errorf("PutMetadata: build store for inode %d: %w", key, err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return nil, fmt.Errorf("PutMetadata: store inode %d: %w", key, err)
	}

	svc := cmd.(*riak.StoreValueCommand)
	vclock := svc.Response.VClock
	if len(vclock) == 0 {
		return nil, fmt.Errorf("PutMetadata: no vclock returned for inode %d", key)
	}
	return vclock, nil
}

// UpdateMetadata overwrites metadata with optimistic concurrency (IfNotModified).
// Fails if the vclock doesn't match, indicating a concurrent modification.
func (kv *RiakKVStore) UpdateMetadata(key uint64, newMeta *bangpb.InodeMeta, vclock_in []byte) ([]byte, error) {
	data, err := proto.Marshal(newMeta)
	if err != nil {
		return nil, fmt.Errorf("UpdateMetadata: marshal inode %d: %w", key, err)
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
		WithIfNotModified(true).
		WithReturnBody(true).
		Build()
	if err != nil {
		return nil, fmt.Errorf("UpdateMetadata: build store for inode %d: %w", key, err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return nil, fmt.Errorf("UpdateMetadata: store inode %d: %w", key, err)
	}
	svc := cmd.(*riak.StoreValueCommand)
	return svc.Response.VClock, err
}

// Metadata fetches inode metadata and its vclock by inode number.
func (kv *RiakKVStore) Metadata(key uint64) (*bangpb.InodeMeta, []byte, error) {
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucketType(kv.metadataBucketType).
		WithBucket(metadataBucket).
		WithKey(fmt.Sprintf("%d", key)).
		Build()
	if err != nil {
		return nil, nil, fmt.Errorf("Metadata: build fetch for inode %d: %w", key, err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return nil, nil, fmt.Errorf("Metadata: fetch inode %d: %w", key, err)
	}

	fvc := cmd.(*riak.FetchValueCommand)
	if fvc.Response == nil || len(fvc.Response.Values) == 0 {
		return nil, nil, fmt.Errorf("Metadata: inode %d not found", key)
	}

	meta := &bangpb.InodeMeta{}
	if err := proto.Unmarshal(fvc.Response.Values[0].Value, meta); err != nil {
		return nil, nil, fmt.Errorf("Metadata: unmarshal inode %d: %w", key, err)
	}
	return meta, fvc.Response.VClock, nil
}

// DeleteMetadata removes inode metadata. If vclockIn is non-nil, the delete
// is conditional on the vclock matching (optimistic concurrency).
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
		return fmt.Errorf("DeleteMetadata: build delete for inode %d: %w", key, err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return fmt.Errorf("DeleteMetadata: delete inode %d: %w", key, err)
	}
	return nil
}

func (kv *RiakKVStore) riakWriteWorker(clustercon *riak.Cluster) {
	kv.writeWg.Add(1)
	defer kv.writeWg.Done()
	defer clustercon.Stop()
	for cmd := range kv.writeQueue {
		var cmderr error
		cmderr = clustercon.Execute(cmd)
		if svc, ok := cmd.(*riak.StoreValueCommand); ok {
			keystr := svc.Response.Values[0].Key
			var key uint64
			if nscan, err := fmt.Sscanf(keystr, "%016x", &key); err != nil {
				kv.errChan <- fmt.Errorf("writeWorker: parse chunk key %q (scanned %d): %v", keystr, nscan, err)
				continue
			}
			if cmderr == nil {
				kv.markClean(key)
				continue
			}
			kv.errChan <- fmt.Errorf("writeWorker: execute: %w", cmderr)
		}
	}
}

func (kv *RiakKVStore) enqueue(cmd riak.Command) error {
	kv.writeQueue <- cmd
	return nil
}

func (kv *RiakKVStore) onEvict(key uint64, chunkEntry chunkEntry) {
	if chunkEntry.dirty {
		obj := &riak.Object{
			Bucket:      chunkBucket,
			BucketType:  kv.chunkBucketType,
			Key:         fmt.Sprintf("%016x", key),
			ContentType: "application/octet-stream",
			Value:       chunkEntry.data,
		}

		cmd, err := riak.NewStoreValueCommandBuilder().
			WithBucketType(kv.chunkBucketType).
			WithBucket(chunkBucket).
			WithContent(obj).
			Build()
		if err != nil {
			kv.errChan <- fmt.Errorf("onEvict: build store for chunk %016x: %w", key, err)
		}

		if err := kv.enqueue(cmd); err != nil {
			kv.errChan <- fmt.Errorf("onEvict: enqueue chunk %016x: %w", key, err)
		}
	}
}

// PutChunk writes a chunk to cache (dirty) and enqueues an async store to Riak.
func (kv *RiakKVStore) PutChunk(key uint64, data []byte) error {
	kv.chunkCache.Add(key, chunkEntry{dirty: true, data: data})
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
		return fmt.Errorf("PutChunk: build store for chunk %016x: %w", key, err)
	}

	if err := kv.enqueue(cmd); err != nil {
		return fmt.Errorf("PutChunk: enqueue chunk %016x: %w", key, err)
	}

	return nil
}

// Chunk retrieves a chunk by key, checking the cache first, then fetching from Riak.
func (kv *RiakKVStore) Chunk(key uint64) ([]byte, error) {
	if kv.chunkCache.Contains(key) {
		dat, ok := kv.chunkCache.Get(key)
		if ok {
			return dat.data, nil
		}
	}

	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucketType(kv.chunkBucketType).
		WithBucket(chunkBucket).
		WithKey(fmt.Sprintf("%016x", key)).
		Build()
	if err != nil {
		return nil, fmt.Errorf("Chunk: build fetch for chunk %016x: %w", key, err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return nil, fmt.Errorf("Chunk: fetch chunk %016x: %w", key, err)
	}

	fvc := cmd.(*riak.FetchValueCommand)
	if fvc.Response == nil || len(fvc.Response.Values) == 0 {
		return nil, fmt.Errorf("Chunk: chunk %016x not found", key)
	}

	data := fvc.Response.Values[0].Value
	kv.chunkCache.Add(key, chunkEntry{dirty: false, data: data})

	return fvc.Response.Values[0].Value, nil
}

// DeleteChunk removes a chunk from cache and enqueues an async delete to Riak.
func (kv *RiakKVStore) DeleteChunk(key uint64) error {
	kv.chunkCache.Remove(key)

	cmd, err := riak.NewDeleteValueCommandBuilder().
		WithBucketType(kv.chunkBucketType).
		WithBucket(chunkBucket).
		WithKey(fmt.Sprintf("%016x", key)).
		Build()
	if err != nil {
		return fmt.Errorf("DeleteChunk: build delete for chunk %016x: %w", key, err)
	}

	if err := kv.enqueue(cmd); err != nil {
		return fmt.Errorf("DeleteChunk: enqueue delete for chunk %016x: %w", key, err)
	}

	return nil
}

// WipeBackend deletes all metadata and chunk keys from Riak.
// Progress is written to w.
func (kv *RiakKVStore) WipeBackend(w io.Writer) error {
	if kv.cluster == nil {
		return fmt.Errorf("WipeBackend: cluster not connected")
	}

	fmt.Fprintf(w, "  wiping metadata [%s/%s]...\n", kv.metadataBucketType, metadataBucket)
	num_meta_keys, err := kv.wipeBucket(w, kv.metadataBucketType, metadataBucket)
	if err != nil {
		return fmt.Errorf("WipeBackend: wipe metadata bucket: %w", err)
	}
	fmt.Fprintf(w, "  deleted %d metadata keys\n", num_meta_keys)

	fmt.Fprintf(w, "  wiping chunks [%s/%s]...\n", kv.chunkBucketType, chunkBucket)
	num_chunk_keys, err := kv.wipeBucket(w, kv.chunkBucketType, chunkBucket)
	if err != nil {
		return fmt.Errorf("WipeBackend: wipe chunk bucket: %w", err)
	}
	fmt.Fprintf(w, "  deleted %d chunk keys\n", num_chunk_keys)

	return nil
}

// wipeBucket lists and deletes all keys in a bucket, fetching vclocks first for clean deletes.
func (kv *RiakKVStore) wipeBucket(w io.Writer, bucketType, bucket string) (int, error) {
	cmd, err := riak.NewListKeysCommandBuilder().
		WithBucketType(bucketType).
		WithBucket(bucket).
		WithStreaming(false).
		Build()
	if err != nil {
		return 0, fmt.Errorf("wipeBucket %s/%s: build list keys: %w", bucketType, bucket, err)
	}

	if err := kv.cluster.Execute(cmd); err != nil {
		return 0, fmt.Errorf("wipeBucket %s/%s: list keys: %w", bucketType, bucket, err)
	}

	lkc := cmd.(*riak.ListKeysCommand)
	if lkc.Response == nil {
		return 0, fmt.Errorf("wipeBucket %s/%s: no keys found", bucketType, bucket)
	}

	total := len(lkc.Response.Keys)
	fmt.Fprintf(w, "  found %d keys in %s/%s\n", total, bucketType, bucket)

	keycount := 0
	for _, key := range lkc.Response.Keys {
		fetch_cmd, err := riak.NewFetchValueCommandBuilder().
			WithBucketType(bucketType).
			WithBucket(bucket).
			WithKey(string(key)).
			Build()
		if err != nil {
			return 0, fmt.Errorf("wipeBucket: build fetch for key %s: %w", key, err)
		}
		if err := kv.cluster.Execute(fetch_cmd); err != nil {
			return 0, fmt.Errorf("wipeBucket: fetch vclock for key %s: %w", key, err)
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
			return 0, fmt.Errorf("wipeBucket: build delete for key %s: %w", key, err)
		}

		if err := kv.cluster.Execute(del_cmd); err != nil {
			return 0, fmt.Errorf("wipeBucket: delete key %s: %w", key, err)
		}
		keycount++
	}

	return keycount, nil
}

// --- DiskUsage: Riak HTTP stats ---
// Block usage/free is synthesized from free disk space on each cluster member (bytes/chunksize).
// It's a rough estimate, not a precise accounting of per-namespace usage.

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
		return nil, fmt.Errorf("DiskUsage: fetch seed stats from %s: %w", seedURL, err)
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
		return nil, fmt.Errorf("DiskUsage: no cluster nodes responded with disk info")
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
		return nil, fmt.Errorf("fetchRiakStats: decode JSON from %s: %w", url, err)
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
		return 0, 0, fmt.Errorf("extractDiskInfo: no disk entries in stats response")
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
