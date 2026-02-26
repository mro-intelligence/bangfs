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

// kv is the package-level KVStore used by node implementations.
// assumes that a given process will only mount one fs!
// TODO: move into the root inode and access with .Root()
var kv KVStore
var inumgen *IdGenerator
var chunkidgen *IdGenerator

const chunksize = 1024 * 8

// BangServer wraps a FUSE server and its backend KV connection.
type BangServer struct {
	*fuse.Server
	kv KVStore
}

// NewBangServer connects to a Riak backend and verifies the filesystem exists.
func NewBangServer(host string, port uint16, namespace string) (*BangServer, error) {
	kvStore, err := NewRiakKVStore(host, port, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to backend: %w", err)
	}
	return NewBangServerWithKV(kvStore)
}

// NewBangServerWithKV creates a BangServer with any KVStore implementation.
func NewBangServerWithKV(kvStore KVStore) (*BangServer, error) {
	// Verify filesystem exists (inode 0)
	if _, _, err := kvStore.Metadata(0); err != nil {
		kvStore.Close()
		return nil, fmt.Errorf("filesystem not initialized (run mkbangfs first): %w", err)
	}

	return &BangServer{kv: kvStore}, nil
}

// Mount mounts a BangFS filesystem at the given mountpoint.
func (bs *BangServer) Mount(mountpoint string) error {
	kv = bs.kv
	inumgen = NewIdGenerator()
	chunkidgen = NewIdGenerator()

	root := &BangDirNode{}
	server, err := fs.Mount(mountpoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: "bangfs",
			Name:   "bangfs",
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
