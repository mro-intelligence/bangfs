package bangfuse

import (
	"bangfs/bangutil"
	"crypto/rand"
	"encoding/binary"
	"os"
	"sync"
	"time"
)

const (
	TIME_BITS = 13
	SEQ_BITS  = 14
)

type IdGenerator struct {
	seq_no   uint64
	local_id uint64
	mut      sync.Mutex
}

// NewIdGenerator initializes an InodeNumGenerator for the current process
func NewIdGenerator() *IdGenerator {
	return &IdGenerator{
		mut:      sync.Mutex{},
		seq_no:   0,
		local_id: uint64(os.Getpid()) ^ getClientID(),
	}
}

// NextId increments the sequence number and returns an inode number based
// on seq_no, current time in milliseconds since epoch, and current server id.
// seq_no increments each time an inode num is generated. The task and server id
// are hashed to save bits
func (ig *IdGenerator) NextId() uint64 {

	// Read and write the seq no, using mutex in case multiple goroutines are trying to increment it at once.
	ig.mut.Lock()
	seq_no := ig.seq_no
	ig.seq_no++
	ig.mut.Unlock()

	ms_since_epoch := time.Now().UnixMilli()

	return uint64(ms_since_epoch) | (seq_no << SEQ_BITS) | (ig.local_id << (TIME_BITS + SEQ_BITS))
}

// =========================
// utility func
// =========================
// getClientId returns a stable server identifier using the following priority:
// 1. BANGFS_CLIENT_ID environment variable (explicit configuration)
// 2. Hash of hostname (works for StatefulSets and regular servers)
// 3. Fall back to random value
func getClientID() uint64 {
	if server_id_str := os.Getenv("BANGFS_CLIENT_ID"); server_id_str != "" {
		return bangutil.FNV64([]byte(server_id_str))
	}
	hostname, err := os.Hostname()
	if err != nil { // TODO: warn that there's no client id defined
		var b [8]byte
		rand.Read(b[:])
		return binary.NativeEndian.Uint64(b[:])
	}
	return uint64(bangutil.FNV32([]byte(hostname)))
}
