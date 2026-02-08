// userspace implementation of the filesystem
// uses go-fuse (github.com/hanwen/go-fuse/v2)
// implements fuse ops that call lower level/utility functions

package fuse

import (
	"context"
	"os"
	"syscall"
	"time"

	bangpb "bangfs/proto"
	"bangfs/util"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// trace returns the global tracer
func trace() *util.Tracer {
	return util.GetTracer()
}

// ===============================================
// BangFile - file node in the filesystem
// ===============================================
type BangFile struct {
	fs.Inode
	kv   *KVStore
	inum uint64
}

var _ = (fs.NodeGetattrer)((*BangFile)(nil))
var _ = (fs.NodeSetattrer)((*BangFile)(nil))
var _ = (fs.NodeOpener)((*BangFile)(nil))
var _ = (fs.NodeReader)((*BangFile)(nil))
var _ = (fs.NodeWriter)((*BangFile)(nil))

// Getattr returns file attributes
func (bf *BangFile) Getattr(ctx context.Context, _ fs.FileHandle, out_attr *fuse.AttrOut) syscall.Errno {
	op := trace().Op("bf.Getattr", bf.inum, "")
	meta, _, err := bf.kv.Metadata(bf.inum)
	if err != nil {
		op.Error(err)
		return syscall.EIO
	}
	MetadataToFuseAttr(bf.inum, meta, &out_attr.Attr)
	op.Done()
	return 0
}

// Setattr sets file attributes (chmod, chown, truncate, utimes)
func (bf *BangFile) Setattr(ctx context.Context, _ fs.FileHandle, in_attr *fuse.SetAttrIn, out_attr *fuse.AttrOut) syscall.Errno {
	meta, vclock, err := bf.kv.Metadata(bf.inum)
	if err != nil {
		return syscall.EIO
	}

	filemeta := meta.GetFile()
	if filemeta == nil {
		return syscall.EINVAL
	}

	// Apply requested changes
	if m, ok := in_attr.GetMode(); ok {
		filemeta.Mode = (filemeta.Mode & syscall.S_IFMT) | (m & ^uint32(syscall.S_IFMT))
	}
	if uid, ok := in_attr.GetUID(); ok {
		filemeta.Uid = uid
	}
	if gid, ok := in_attr.GetGID(); ok {
		filemeta.Gid = gid
	}
	if size, ok := in_attr.GetSize(); ok {
		// Truncate - for now just update size, chunks handled separately
		filemeta.Size = size
		// TODO: actually truncate chunks when chunk ops are implemented
	}
	if atime, ok := in_attr.GetATime(); ok {
		filemeta.AtimeNs = atime.UnixNano()
	}
	if mtime, ok := in_attr.GetMTime(); ok {
		filemeta.MtimeNs = mtime.UnixNano()
	}

	filemeta.CtimeNs = time.Now().UnixNano()

	if err := bf.kv.UpdateMetadata(bf.inum, meta, vclock); err != nil {
		return syscall.EIO
	}

	MetadataToFuseAttr(bf.inum, meta, &out_attr.Attr)
	return 0
}

// Open opens a file
func (bf *BangFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// TODO: track open files, handle O_TRUNC etc
	return nil, 0, 0
}

// Read reads data from a file
func (bf *BangFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, offst int64) (fuse.ReadResult, syscall.Errno) {
	// TODO: implement chunk reading from backend
	// For now return empty - will implement when doing chunk ops
	return fuse.ReadResultData([]byte{}), 0
}

// Write writes data to a file
func (bf *BangFile) Write(ctx context.Context, fh fs.FileHandle, data []byte, offst int64) (bytes_written uint32, errno syscall.Errno) {
	// TODO: implement chunk writing to backend
	// For now return EROFS - will implement when doing chunk ops
	return 0, syscall.EROFS
}

// ===============================================
// BangDirNode - directory node in the filesystem
// ===============================================
type BangDirNode struct {
	BangFile
}

// Verify interface compliance
var _ = (fs.NodeGetattrer)((*BangDirNode)(nil))
var _ = (fs.NodeSetattrer)((*BangDirNode)(nil))
var _ = (fs.NodeLookuper)((*BangDirNode)(nil))
var _ = (fs.NodeReaddirer)((*BangDirNode)(nil))
var _ = (fs.NodeCreater)((*BangDirNode)(nil))
var _ = (fs.NodeMkdirer)((*BangDirNode)(nil))
var _ = (fs.NodeUnlinker)((*BangDirNode)(nil))
var _ = (fs.NodeRmdirer)((*BangDirNode)(nil))
var _ = (fs.NodeRenamer)((*BangDirNode)(nil))

// Lookup finds a child by name
func (d *BangDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	op := trace().Op("Lookup", d.inum, name)
	dirMeta, _, err := d.kv.Metadata(d.inum)
	if err != nil {
		op.Error(err)
		return nil, syscall.EIO
	}

	dir := dirMeta.GetDir()
	if dir == nil {
		return nil, syscall.ENOTDIR
	}

	// Search children for matching name
	for _, childInum := range dir.Children {
		childMeta, _, err := d.kv.Metadata(childInum)
		if err != nil {
			continue
		}

		var childName string
		var isDir bool
		switch m := childMeta.Meta.(type) {
		case *bangpb.InodeMeta_Dir:
			childName = m.Dir.Name
			isDir = true
		case *bangpb.InodeMeta_File:
			childName = m.File.Name
			isDir = false
		}

		if childName == name {
			MetadataToFuseAttr(childInum, childMeta, &out.Attr)

			var child fs.InodeEmbedder
			var mode uint32
			if isDir {
				child = &BangDirNode{BangFile: BangFile{kv: d.kv, inum: childInum}}
				mode = syscall.S_IFDIR
			} else {
				child = &BangFile{kv: d.kv, inum: childInum}
				mode = syscall.S_IFREG
			}
			op.Done()
			return d.NewInode(ctx, child, fs.StableAttr{Mode: mode, Ino: childInum}), 0
		}
	}

	op.Errno(syscall.ENOENT)
	return nil, syscall.ENOENT
}

// Readdir lists directory contents
func (d *BangDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	dirMeta, _, err := d.kv.Metadata(d.inum)
	if err != nil {
		return nil, syscall.EIO
	}

	dir := dirMeta.GetDir()
	if dir == nil {
		return nil, syscall.ENOTDIR
	}

	entries := []fuse.DirEntry{}

	for _, childInum := range dir.Children {
		childMeta, _, err := d.kv.Metadata(childInum)
		if err != nil {
			continue
		}

		var entry fuse.DirEntry
		entry.Ino = childInum
		switch m := childMeta.Meta.(type) {
		case *bangpb.InodeMeta_Dir:
			entry.Name = m.Dir.Name
			entry.Mode = m.Dir.Mode
		case *bangpb.InodeMeta_File:
			entry.Name = m.File.Name
			entry.Mode = m.File.Mode
		}
		entries = append(entries, entry)
	}

	return fs.NewListDirStream(entries), 0
}

// Create creates a new file in the directory
func (d *BangDirNode) Create(ctx context.Context, name string, _ uint32 /*flags*/, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	op := trace().Op("Create", d.inum, name)
	// Check if name already exists
	dirMeta, dirVClock, err := d.kv.Metadata(d.inum)
	if err != nil {
		op.Error(err)
		return nil, nil, 0, syscall.EIO
	}
	dir := dirMeta.GetDir()
	if dir == nil {
		return nil, nil, 0, syscall.ENOTDIR
	}

	for _, childInum := range dir.Children {
		childMeta, _, _ := d.kv.Metadata(childInum)
		if childMeta != nil {
			var childName string
			switch m := childMeta.Meta.(type) {
			case *bangpb.InodeMeta_Dir:
				childName = m.Dir.Name
			case *bangpb.InodeMeta_File:
				childName = m.File.Name
			}
			if childName == name {
				return nil, nil, 0, syscall.EEXIST
			}
		}
	}

	// Allocate new inode
	newInum, err := d.kv.AllocInode()
	if err != nil {
		op.Error(err)
		return nil, nil, 0, syscall.EIO
	}

	// Create file metadata
	now := time.Now().UnixNano()
	fileMeta := &bangpb.InodeMeta{
		Meta: &bangpb.InodeMeta_File{
			File: &bangpb.FileMeta{
				Name:        name,
				ParentInode: d.inum,
				Mode:        mode | syscall.S_IFREG,
				Uid:         uint32(os.Getuid()),
				Gid:         uint32(os.Getgid()),
				Size:        0,
				CtimeNs:     now,
				MtimeNs:     now,
				AtimeNs:     now,
				Chunks:      []*bangpb.ChunkRef{},
				Nlink:       1,
				Version:     1,
			},
		},
	}

	if err := d.kv.PutMetadata(newInum, fileMeta); err != nil {
		op.Error(err)
		return nil, nil, 0, syscall.EIO
	}

	// Add to parent's children
	dir.Children = append(dir.Children, newInum)
	dir.MtimeNs = now
	dir.CtimeNs = now
	if err := d.kv.UpdateMetadata(d.inum, dirMeta, dirVClock); err != nil {
		op.Error(err)
		return nil, nil, 0, syscall.EIO
	}

	MetadataToFuseAttr(newInum, fileMeta, &out.Attr)
	newFile := &BangFile{kv: d.kv, inum: newInum}
	op.Done()
	return d.NewInode(ctx, newFile, fs.StableAttr{Mode: syscall.S_IFREG, Ino: newInum}), nil, 0, 0
}

// Mkdir creates a new directory
func (d *BangDirNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	op := trace().Op("Mkdir", d.inum, name)
	// Check if name already exists
	dirMeta, dirVClock, err := d.kv.Metadata(d.inum)
	if err != nil {
		op.Error(err)
		return nil, syscall.EIO
	}
	dir := dirMeta.GetDir()
	if dir == nil {
		return nil, syscall.ENOTDIR
	}

	for _, childInum := range dir.Children {
		childMeta, _, _ := d.kv.Metadata(childInum)
		if childMeta != nil {
			var childName string
			switch m := childMeta.Meta.(type) {
			case *bangpb.InodeMeta_Dir:
				childName = m.Dir.Name
			case *bangpb.InodeMeta_File:
				childName = m.File.Name
			}
			if childName == name {
				return nil, syscall.EEXIST
			}
		}
	}

	// Allocate new inode
	newInum, err := d.kv.AllocInode()
	if err != nil {
		op.Error(err)
		return nil, syscall.EIO
	}

	// Create directory metadata
	now := time.Now().UnixNano()
	newDirMeta := &bangpb.InodeMeta{
		Meta: &bangpb.InodeMeta_Dir{
			Dir: &bangpb.DirMeta{
				Name:        name,
				ParentInode: d.inum,
				Mode:        mode | syscall.S_IFDIR,
				Uid:         uint32(os.Getuid()),
				Gid:         uint32(os.Getgid()),
				CtimeNs:     now,
				MtimeNs:     now,
				AtimeNs:     now,
				Children:    []uint64{},
				Nlink:       2,
				Version:     1,
			},
		},
	}

	if err := d.kv.PutMetadata(newInum, newDirMeta); err != nil {
		op.Error(err)
		return nil, syscall.EIO
	}

	// Add to parent's children and increment nlink
	dir.Children = append(dir.Children, newInum)
	dir.MtimeNs = now
	dir.CtimeNs = now
	dir.Nlink++
	if err := d.kv.UpdateMetadata(d.inum, dirMeta, dirVClock); err != nil {
		op.Error(err)
		return nil, syscall.EIO
	}

	MetadataToFuseAttr(newInum, newDirMeta, &out.Attr)
	newDir := &BangDirNode{BangFile: BangFile{kv: d.kv, inum: newInum}}
	op.Done()
	return d.NewInode(ctx, newDir, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: newInum}), 0
}

// Unlink removes a file
func (d *BangDirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	op := trace().Op("Unlink", d.inum, name)
	dirMeta, dirVClock, err := d.kv.Metadata(d.inum)
	if err != nil {
		op.Error(err)
		return syscall.EIO
	}
	dir := dirMeta.GetDir()
	if dir == nil {
		op.Errno(syscall.ENOTDIR)
		return syscall.ENOTDIR
	}

	// Find the child
	var targetInum uint64
	var targetIdx int = -1
	for i, childInum := range dir.Children {
		childMeta, _, err := d.kv.Metadata(childInum)
		if err != nil {
			continue
		}
		switch m := childMeta.Meta.(type) {
		case *bangpb.InodeMeta_File:
			if m.File.Name == name {
				targetInum = childInum
				targetIdx = i
			}
		case *bangpb.InodeMeta_Dir:
			if m.Dir.Name == name {
				op.Errno(syscall.EISDIR)
				return syscall.EISDIR // can't unlink a directory
			}
		}
	}

	if targetIdx == -1 {
		op.Errno(syscall.ENOENT)
		return syscall.ENOENT
	}

	// Remove from parent's children
	dir.Children = append(dir.Children[:targetIdx], dir.Children[targetIdx+1:]...)
	now := time.Now().UnixNano()
	dir.MtimeNs = now
	dir.CtimeNs = now
	if err := d.kv.UpdateMetadata(d.inum, dirMeta, dirVClock); err != nil {
		op.Error(err)
		return syscall.EIO
	}

	// Delete the file metadata (chunks become orphaned, GC will clean up)
	if err := d.kv.DeleteMetadata(targetInum); err != nil {
		op.Error(err)
		return syscall.EIO
	}

	op.Done()
	return 0
}

// Rmdir removes an empty directory
func (d *BangDirNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	op := trace().Op("Rmdir", d.inum, name)
	dirMeta, dirVClock, err := d.kv.Metadata(d.inum)
	if err != nil {
		op.Error(err)
		return syscall.EIO
	}
	dir := dirMeta.GetDir()
	if dir == nil {
		op.Errno(syscall.ENOTDIR)
		return syscall.ENOTDIR
	}

	// Find the child directory
	var targetInum uint64
	var targetIdx int = -1
	for i, childInum := range dir.Children {
		childMeta, _, err := d.kv.Metadata(childInum)
		if err != nil {
			continue
		}
		switch m := childMeta.Meta.(type) {
		case *bangpb.InodeMeta_Dir:
			if m.Dir.Name == name {
				// Check if empty
				if len(m.Dir.Children) > 0 {
					op.Errno(syscall.ENOTEMPTY)
					return syscall.ENOTEMPTY
				}
				targetInum = childInum
				targetIdx = i
			}
		case *bangpb.InodeMeta_File:
			if m.File.Name == name {
				op.Errno(syscall.ENOTDIR)
				return syscall.ENOTDIR
			}
		}
	}

	if targetIdx == -1 {
		op.Errno(syscall.ENOENT)
		return syscall.ENOENT
	}

	// Remove from parent's children and decrement nlink
	dir.Children = append(dir.Children[:targetIdx], dir.Children[targetIdx+1:]...)
	now := time.Now().UnixNano()
	dir.MtimeNs = now
	dir.CtimeNs = now
	dir.Nlink--
	if err := d.kv.UpdateMetadata(d.inum, dirMeta, dirVClock); err != nil {
		op.Error(err)
		return syscall.EIO
	}

	// Delete the directory metadata
	if err := d.kv.DeleteMetadata(targetInum); err != nil {
		op.Error(err)
		return syscall.EIO
	}

	op.Done()
	return 0
}

// Rename moves/renames a file or directory
func (d *BangDirNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	// Get source directory metadata
	srcDirMeta, srcDirVClock, err := d.kv.Metadata(d.inum)
	if err != nil {
		return syscall.EIO
	}
	srcDir := srcDirMeta.GetDir()
	if srcDir == nil {
		return syscall.ENOTDIR
	}

	// Find source child
	var srcInum uint64
	var srcIdx int = -1
	var srcIsDir bool
	for i, childInum := range srcDir.Children {
		childMeta, _, err := d.kv.Metadata(childInum)
		if err != nil {
			continue
		}
		var childName string
		switch m := childMeta.Meta.(type) {
		case *bangpb.InodeMeta_Dir:
			childName = m.Dir.Name
			if childName == name {
				srcIsDir = true
			}
		case *bangpb.InodeMeta_File:
			childName = m.File.Name
		}
		if childName == name {
			srcInum = childInum
			srcIdx = i
			break
		}
	}

	if srcIdx == -1 {
		return syscall.ENOENT
	}

	// Get destination parent
	dstParent, ok := newParent.(*BangDirNode)
	if !ok {
		return syscall.EINVAL
	}

	dstDirMeta, dstDirVClock, err := d.kv.Metadata(dstParent.inum)
	if err != nil {
		return syscall.EIO
	}
	dstDir := dstDirMeta.GetDir()
	if dstDir == nil {
		return syscall.ENOTDIR
	}

	// Check if destination name exists and remove it
	for i, childInum := range dstDir.Children {
		childMeta, _, err := d.kv.Metadata(childInum)
		if err != nil {
			continue
		}
		var childName string
		var isDir bool
		switch m := childMeta.Meta.(type) {
		case *bangpb.InodeMeta_Dir:
			childName = m.Dir.Name
			isDir = true
		case *bangpb.InodeMeta_File:
			childName = m.File.Name
		}
		if childName == newName {
			if isDir {
				return syscall.EISDIR // can't overwrite directory
			}
			// Remove existing file
			dstDir.Children = append(dstDir.Children[:i], dstDir.Children[i+1:]...)
			d.kv.DeleteMetadata(childInum)
			break
		}
	}

	// Update source metadata with new name and parent
	srcMeta, srcVClock, err := d.kv.Metadata(srcInum)
	if err != nil {
		return syscall.EIO
	}

	now := time.Now().UnixNano()
	switch m := srcMeta.Meta.(type) {
	case *bangpb.InodeMeta_Dir:
		m.Dir.Name = newName
		m.Dir.ParentInode = dstParent.inum
		m.Dir.CtimeNs = now
	case *bangpb.InodeMeta_File:
		m.File.Name = newName
		m.File.ParentInode = dstParent.inum
		m.File.CtimeNs = now
	}

	if err := d.kv.UpdateMetadata(srcInum, srcMeta, srcVClock); err != nil {
		return syscall.EIO
	}

	// Remove from source parent
	srcDir.Children = append(srcDir.Children[:srcIdx], srcDir.Children[srcIdx+1:]...)
	srcDir.MtimeNs = now
	srcDir.CtimeNs = now
	if srcIsDir {
		srcDir.Nlink--
	}

	// Add to destination parent
	dstDir.Children = append(dstDir.Children, srcInum)
	dstDir.MtimeNs = now
	dstDir.CtimeNs = now
	if srcIsDir {
		dstDir.Nlink++
	}

	// Save both parents (might be same directory)
	if err := d.kv.UpdateMetadata(d.inum, srcDirMeta, srcDirVClock); err != nil {
		return syscall.EIO
	}
	if dstParent.inum != d.inum {
		if err := d.kv.UpdateMetadata(dstParent.inum, dstDirMeta, dstDirVClock); err != nil {
			return syscall.EIO
		}
	}

	return 0
}

// ===============================================
// Server wraps the FUSE server
// ===============================================
type Server struct {
	server *fuse.Server
	kv     *KVStore
}

func (s *Server) Wait() {
	s.server.Wait()
}

func (s *Server) Unmount() error {
	return s.server.Unmount()
}

// Mount mounts the BangFS filesystem at the specified mountpoint
func Mount(mountpoint string, kv *KVStore) (*Server, error) {
	root := &BangDirNode{BangFile: BangFile{kv: kv, inum: 0}}

	server, err := fs.Mount(mountpoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			Name:  "bangfs",
			Debug: false,
		},
	})
	if err != nil {
		return nil, err
	}

	return &Server{server: server, kv: kv}, nil
}
