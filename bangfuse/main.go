// userspace implementation of the filesystem
// uses go-fuse (github.com/hanwen/go-fuse/v2)
// implements fuse ops that call lower level/utility functions

package bangfuse

import (
	"fmt"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// gKVStore is the package-level KVStore used by node implementations.
// assumes that a given process will only mount one fs!
// TODO: move into the root inode and access with .Root()
var gKVStore KVStore
var gInumgen *IdGenerator
var gChunkidgen *IdGenerator

const defaultChunkSize = 1024 * 1024 // 1MB

var gChunksize uint32 = defaultChunkSize

func SetChunkSize(size uint32) { gChunksize = size }
func GetChunkSize() uint32     { return gChunksize }

// BangServer wraps a FUSE server and its backend KV connection.
type BangServer struct {
	*fuse.Server
	kv KVStore
}

// NewBangServer creates a BangServer with any KVStore implementation.
func NewBangServer(kvStore KVStore) (*BangServer, error) {
	// Verify filesystem exists (inode 0) and read chunk size
	rootMeta, _, err := kvStore.Metadata(0)
	if err != nil {
		kvStore.Close()
		return nil, fmt.Errorf("filesystem not initialized (run mkbangfs first): %w", err)
	}
	if rootMeta.BlockSize > 0 {
		SetChunkSize(rootMeta.BlockSize)
	} else {
		return nil, fmt.Errorf("Could not determine the chunk size")
	}

	return &BangServer{kv: kvStore}, nil
}

// Mount mounts a BangFS filesystem at the given mountpoint.
func (bs *BangServer) Mount(mountpoint string) error {
	gKVStore = bs.kv
	gInumgen = NewIdGenerator()
	gChunkidgen = NewIdGenerator()

	root := &BangDirNode{}
	server, err := fs.Mount(mountpoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName:      "bangfs",
			Name:        "bangfs",
			EnableLocks: false, // Locks not implemented
			//Debug:          true,
			SingleThreaded: false,
			//In Linux 4.20 and later, the value
			//   can go up to 1 MiB and go-fuse calculates the MaxPages value acc.
			//   to MaxWrite, rounding up.
			MaxWrite:      int(GetChunkSize()),
			DisableXAttrs: true,
			//Logger:        nil,

			// If set, ask kernel not to do automatic data cache invalidation.
			// The filesystem is fully responsible for invalidating data cache.
			//ExplicitDataCacheControl false,
		},
		RootStableAttr: &fs.StableAttr{
			Mode: syscall.S_IFDIR,
		},
	})
	if err != nil {
		return err
	}

	bs.Server = server
	return nil
}

// Close shuts down the backend connection.
func (bs *BangServer) Close() error {
	return bs.kv.Close()
}
