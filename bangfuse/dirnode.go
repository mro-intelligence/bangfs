package bangfuse

import (
	"bangfs/bangutil"
	bangpb "bangfs/proto"
	"context"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// BangDirNode implements inode ops for directories.
type BangDirNode struct {
	BangFileNode // Superset of BangFileNode methods
}

// Verify interface compliance
var _ = (fs.NodeReaddirer)((*BangDirNode)(nil))
var _ = (fs.NodeCreater)((*BangDirNode)(nil))
var _ = (fs.NodeMkdirer)((*BangDirNode)(nil))
var _ = (fs.NodeLookuper)((*BangDirNode)(nil))
var _ = (fs.NodeRmdirer)((*BangDirNode)(nil))
var _ = (fs.NodeUnlinker)((*BangDirNode)(nil))

// Readdir lists directory contents and prepends . (self inode) and .. (parent inode) to the real children.
func (d *BangDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {

	inum := d.StableAttr().Ino
	op := bangutil.GetTracer().Op("Readdir", inum, "")

	dir_meta, _, err := gKVStore.Metadata(inum)
	if err != nil {
		op.Error(err)
		return nil, syscall.EIO
	}
	if !IsDir(dir_meta) {
		op.Error(fmt.Errorf("inode %d is not a directory (mode: %o)", inum, dir_meta.Mode))
		return nil, syscall.EINVAL // TODO: approrpirate err cde
	}

	// . and .. must be returned by the FUSE server; the kernel doesn't add them
	entries := []fuse.DirEntry{
		{Name: ".", Ino: inum, Mode: dir_meta.Mode},
		{Name: "..", Ino: dir_meta.ParentInode, Mode: syscall.S_IFDIR},
	}
	for _, child := range dir_meta.GetChildEntries() {
		child_meta, _, err := gKVStore.Metadata(child.Inode)
		if err != nil {
			op.Error(err)
			continue // TODO: handle error
		}
		entries = append(entries, fuse.DirEntry{
			Ino:  child.Inode,
			Name: child_meta.Name,
			Mode: child_meta.Mode,
		})
	}

	op.Done()
	return fs.NewListDirStream(entries), 0 // TODO: make this an actual stream?
}

// Create creates a regular file
func (d *BangDirNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {

	inum := d.StableAttr().Ino
	op := bangutil.GetTracer().Op("Create", inum, name)

	// Read the directory children from the backend.
	dir_meta, dir_vclock, err := gKVStore.Metadata(inum)
	if err != nil {
		op.Error(fmt.Errorf("getting metadata: %v", err))
		return nil, nil, 0, syscall.EIO
	}
	child_entries := dir_meta.GetChildEntries()
	for _, c := range child_entries {
		if name == c.Name {
			return nil, nil, 0, syscall.EEXIST
		}
	}

	// Create a new backend metadata struct for the new file and store it in the backend.
	now := time.Now().UnixNano() // TODO: check why time fields don't use uint64 but int64
	new_inum := gInumgen.NextId()
	new_file_meta := &bangpb.InodeMeta{
		Name:        name,
		ParentInode: inum,
		Mode:        mode | syscall.S_IFREG,
		Uid:         uint32(os.Getuid()),
		Gid:         uint32(os.Getgid()),
		CtimeNs:     now,
		MtimeNs:     now,
		AtimeNs:     now,
		Chunks:      []*bangpb.ChunkRef{},
		Nlink:       1,
	}
	var new_vclock []byte
	new_vclock, err = gKVStore.PutMetadata(new_inum, new_file_meta)
	if err != nil {
		op.Error(fmt.Errorf("storing the new file metadata: %v", err))
		return nil, nil, 0, syscall.EIO
	}

	// Add the new file link to the directory and try to update it.
	// TODO: address possible condition if the directory metadata has been concurrently modified.
	dir_meta.ChildEntries = append(child_entries, &bangpb.ChildEntry{Name: name, Inode: new_inum})
	dir_meta.MtimeNs = now
	dir_meta.CtimeNs = now
	_, err = gKVStore.UpdateMetadata(inum, dir_meta, dir_vclock)
	if err != nil {
		op.Error(fmt.Errorf("updating metadata for dir inode: %v", err))
		return nil, nil, 0, syscall.EIO
	}

	inode := d.NewInode(ctx, &BangFileNode{} /* ops */, fs.StableAttr{Mode: syscall.S_IFREG, Ino: new_inum})
	fh = &BangFH{
		Inum:     new_inum,
		VClock:   new_vclock,
		Metadata: new_file_meta,
		Flags:    flags,
	}

	op.Done()
	return inode, fh, 0, 0
}

// Mkdir creates a new directory
func (d *BangDirNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	inum := d.StableAttr().Ino
	op := bangutil.GetTracer().Op("Mkdir", inum, name)

	// Read the directory children from the backend.
	dir_meta, vclock, err := gKVStore.Metadata(inum)
	if err != nil {
		op.Error(fmt.Errorf("getting metadata: %v", err))
		return nil, syscall.EIO
	}
	// TODO: check Mkdir (and Create) should check for existing entries? or does Lookup get called?
	child_entries := dir_meta.GetChildEntries()
	for _, c := range child_entries {
		if name == c.Name {
			op.Error(fmt.Errorf("exists: %v", err))
			return nil, syscall.EEXIST
		}
	}

	// Create a new backend metadata struct for the new file and store it in the backend.
	now := time.Now().UnixNano() // TODO: check why time fields don't use uint64 but int64
	new_inum := gInumgen.NextId()
	new_dir_meta := &bangpb.InodeMeta{
		Name:         name,
		ParentInode:  inum,
		Mode:         mode | syscall.S_IFDIR,
		Uid:          uint32(os.Getuid()),
		Gid:          uint32(os.Getgid()),
		CtimeNs:      now,
		MtimeNs:      now,
		AtimeNs:      now,
		ChildEntries: []*bangpb.ChildEntry{},
		Nlink:        2, // . (self) + entry in parent
	}
	_, err = gKVStore.PutMetadata(new_inum, new_dir_meta)
	if err != nil {
		op.Error(fmt.Errorf("storing the new dir metadata: %v", err))
		return nil, syscall.EIO
	}

	// Add the new file link to the directory and try to update it.
	// TODO: address possible condition if the directory metadata has been concurrently modified.
	dir_meta.ChildEntries = append(child_entries, &bangpb.ChildEntry{Name: name, Inode: new_inum})
	dir_meta.MtimeNs = now
	dir_meta.CtimeNs = now
	dir_meta.Nlink++ // new subdir's ".." points back to us
	_, err = gKVStore.UpdateMetadata(inum, dir_meta, vclock)
	if err != nil {
		op.Error(fmt.Errorf("updating metadata for dir inode: %v", err))
		return nil, syscall.EIO
	}

	inode := d.NewInode(ctx, &BangDirNode{} /* ops */, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: new_inum})
	op.Done()
	return inode, 0
}

// Lookup looks up child inodes (files or directories) by name
func (d *BangDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	inum := d.StableAttr().Ino
	op := bangutil.GetTracer().Op("Lookup", inum, name)

	// Read the directory children from the backend.
	dir_meta, _, err := gKVStore.Metadata(inum)
	if err != nil {
		op.Error(fmt.Errorf("getting metadata: %v", err))
		return nil, syscall.EIO
	}

	// Iterate through to find the file
	// TODO: make a more efficient data structure. This is a linear search.
	child_entries := dir_meta.GetChildEntries()
	for _, c := range child_entries {
		if name == c.Name {
			found_inum := c.Inode
			found_meta, _, err := gKVStore.Metadata(found_inum)
			if err != nil {
				op.Error(fmt.Errorf("getting metadata for found inode"))
				return nil, syscall.EIO
			}
			// TODO: check why return a NewInode and set the mode and ops, maybe the number is sufficient.
			if IsDir(found_meta) {
				return d.NewInode(ctx, &BangDirNode{}, fs.StableAttr{Mode: fuse.S_IFDIR, Ino: found_inum}), 0
			} else if IsFile(found_meta) {
				return d.NewInode(ctx, &BangFileNode{}, fs.StableAttr{Mode: fuse.S_IFREG, Ino: found_inum}), 0
			} else {
				op.Error(fmt.Errorf("found inode is neither file nor directory"))
				return nil, syscall.ENOTSUP
			}
		}
	}

	op.Error(fmt.Errorf("could not find file"))
	return nil, syscall.ENOENT
}

func (d *BangDirNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	inum := d.StableAttr().Ino
	op := bangutil.GetTracer().Op("Rmdir", inum, name)

	// TODO: check if needed to verify if its a directory

	// Read the directory children from the backend.
	dir_meta, vclock, err := gKVStore.Metadata(inum)
	if err != nil {
		op.Error(fmt.Errorf("getting metadata: %v", err))
		return syscall.EIO
	}
	// TODO: check Mkdir (and Create) should check for existing entries? or does Lookup get called?
	child_entries := dir_meta.GetChildEntries()
	new_child_entries := []*bangpb.ChildEntry{}
	found := false
	var inum_to_delete uint64
	for _, c := range child_entries {
		if name == c.Name {
			found = true
			inum_to_delete = c.Inode
			continue
		}
		new_child_entries = append(new_child_entries, c)
	}
	if !found {
		op.Error(fmt.Errorf("not found"))
		return syscall.ENOENT
	}

	// Check if the directory is empty
	to_delete_meta, to_delete_vclock, err := gKVStore.Metadata(inum_to_delete)
	if err != nil {
		op.Error(fmt.Errorf("retrieving child directory metadata: %v", err))
		return syscall.EIO
	}
	if len(to_delete_meta.GetChildEntries()) > 0 {
		op.Error(fmt.Errorf("directory not empty"))
		return syscall.ENOTEMPTY
	}

	// TODO: correctly modify inode change time here
	dir_meta.ChildEntries = new_child_entries
	dir_meta.Nlink-- // removed subdir's ".." no longer points to us
	_, err = gKVStore.UpdateMetadata(inum, dir_meta, vclock)
	if err != nil {
		op.Error(fmt.Errorf("updating metadata for dir inode: %v", err))
		return syscall.EIO
	}

	if err = gKVStore.DeleteMetadata(inum_to_delete, to_delete_vclock); err != nil {
		op.Error(fmt.Errorf("deleting child dir metadata: %v", err))
	}

	op.Done()
	return 0
}

// Unlink removes a child file entry from the directory inode
func (d *BangDirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	inum := d.StableAttr().Ino
	op := bangutil.GetTracer().Op("Unlink", inum, name)

	// Read the directory children from the backend.
	dirMeta, vclock, err := gKVStore.Metadata(inum)
	if err != nil {
		op.Error(fmt.Errorf("getting metadata: %v", err))
		return syscall.EIO
	}

	// TODO: check that this is a regular file? the kernel seems to do this already.

	// TODO: check Mkdir (and Create) should check existing entries? or does Lookup get called?
	child_entries := dirMeta.GetChildEntries()
	updated_child_entries := []*bangpb.ChildEntry{}
	found := false
	var inum_to_delete uint64
	for _, c := range child_entries {
		if name == c.Name {
			found = true
			inum_to_delete = c.Inode
			continue
		}
		updated_child_entries = append(updated_child_entries, c)
	}
	if !found {
		op.Error(fmt.Errorf("not found"))
		return syscall.ENOENT
	}

	dirMeta.ChildEntries = updated_child_entries
	//dirMeta.MtimeNs = now // TODO: check which of these to modify
	//dirMeta.CtimeNs = now
	_, err = gKVStore.UpdateMetadata(inum, dirMeta, vclock)
	if err != nil {
		op.Error(fmt.Errorf("updating metadata for dir inode: %v", err))
		return syscall.EIO
	}

	// Look up the file that we unlinked (now orphaned since we dont have hardlinks)
	unlinked_file_meta, unlinked_file_vclock, err := gKVStore.Metadata(inum_to_delete)
	if err != nil {
		op.Error(fmt.Errorf("lookup of unlinked file"))
		return syscall.EIO
	}

	// Delete the chunks. Don't bother updating the metadata since it gets deleted below.
	chunkRefs := unlinked_file_meta.Chunks
	for _, c := range chunkRefs {
		chunk_key := c.Hash
		if err = gKVStore.DeleteChunk(chunk_key); err != nil {
			op.Error(fmt.Errorf("deleting chunk %v", chunk_key)) // garbage collect later
		}
	}

	// Delete the file metadata itself
	// TODO: make this work even if the file inode changed between reading and deleting it
	if err = gKVStore.DeleteMetadata(inum_to_delete, unlinked_file_vclock); err != nil {
		op.Error(fmt.Errorf("deleting file metadata: %v", err))
	}

	op.Done()
	return 0
}
