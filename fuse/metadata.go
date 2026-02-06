// Utility functions to read/write file metadata to the backend
package fuse

import (
	pb "bangfs/proto"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// NewMetaDir creates a new metadata directory thats blank
func NewMetaDir() {
	// TODO
}

// =========== conversion functions =============
//
//	convert between the pb and the structs used in the go-fuse

// MetaToFuse fills a FUSE Attr struct from BangFS protobuf metadata.
//
// Parameters:
//   - inum: the inode number (stored as KV key, not in proto)
//   - meta: the protobuf metadata from the KV store
//   - out: the FUSE Attr struct to populate
//
// WHY THIS EXISTS:
// BangFS stores file/directory metadata in protobuf format (proto/metadata.proto)
// using pb.InodeMeta (with pb.FileMeta or pb.DirMeta inside).
// FUSE requires attributes in its own fuse.Attr struct format.
// This function bridges the two representations.
//
// The inode number is passed separately because it's the KV key, not stored
// in the protobuf value (avoids redundant storage).
//
// IF YOU CHANGE THE PROTOBUF SCHEMA:
// Update toAttrInternal() below to map any new/changed fields.
// The protobuf uses nanosecond timestamps (AtimeNs, MtimeNs, CtimeNs),
// while FUSE splits them into seconds + nanoseconds fields.
func MetadataToFuseAttr(inum uint64, meta *pb.InodeMeta, out *fuse.Attr) {
	out.Ino = inum
	switch m := meta.Meta.(type) {
	case *pb.InodeMeta_File:
		out.Mode = m.File.Mode
		out.Uid = m.File.Uid
		out.Gid = m.File.Gid
		out.Size = m.File.Size
		out.Nlink = m.File.Nlink
		out.Atime = uint64(m.File.AtimeNs / 1e9)
		out.Atimensec = uint32(m.File.AtimeNs % 1e9)
		out.Mtime = uint64(m.File.MtimeNs / 1e9)
		out.Mtimensec = uint32(m.File.MtimeNs % 1e9)
		out.Ctime = uint64(m.File.CtimeNs / 1e9)
		out.Ctimensec = uint32(m.File.CtimeNs % 1e9)
	case *pb.InodeMeta_Dir:
		out.Mode = m.Dir.Mode
		out.Uid = m.Dir.Uid
		out.Gid = m.Dir.Gid
		out.Nlink = m.Dir.Nlink
		out.Atime = uint64(m.Dir.AtimeNs / 1e9)
		out.Atimensec = uint32(m.Dir.AtimeNs % 1e9)
		out.Mtime = uint64(m.Dir.MtimeNs / 1e9)
		out.Mtimensec = uint32(m.Dir.MtimeNs % 1e9)
		out.Ctime = uint64(m.Dir.CtimeNs / 1e9)
		out.Ctimensec = uint32(m.Dir.CtimeNs % 1e9)
	}
}

// FromInode converts FUSE inode to protobuf metadata (stub)
func FromInode(i *fs.Inode) *pb.InodeMeta {
	// TODO: implement - need to track metadata separately from fs.Inode
	return nil
}

// ============ access the backend ==========
// CRUD

// CreateBackendInode creates inode in the kv store with key inode_num
func CreateBackendInode(inum uint32, upd *pb.InodeMeta) error {
	// TODO: implement with kvstore
	return nil
}

// ReadBackendInode reads inode from the kv store
func ReadBackendInode(inum uint32) (*pb.InodeMeta, error) {
	// TODO: implement with kvstore
	return nil, nil
}

// UpdateBackendInode updates inode in the kv store
func UpdateBackendInode(inum uint32, upd *pb.InodeMeta) error {
	// TODO: implement with kvstore
	return nil
}

// DeleteBackendInode deletes inode from the kv store
func DeleteBackendInode(inum uint32) error {
	// TODO: implement with kvstore
	return nil
}

// === new fs ====

// NewFs initializes a new empty filesystem
func NewFs() error {
	// calls WipeFs
	// makes a new Inode to represent the root dir of an empty fs
	// should this be in bangfs program or moved to another executable?
	// note: FUSE handles '.' and '..' automatically
	return nil
}
