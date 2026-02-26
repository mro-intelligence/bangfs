package bangfuse

import (
	"bangfs/bangutil"
	"context"
	"fmt"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func debugSetAttrIn(in *fuse.SetAttrIn) string {
	var parts []string

	trunc := func(s string) string { return fmt.Sprintf("%s...", s[0:10]) }
	if mode, ok := in.GetMode(); ok {
		parts = append(parts, fmt.Sprintf("mode=0%o", mode))
	}
	if size, ok := in.GetSize(); ok {
		parts = append(parts, fmt.Sprintf("size=%d", size))
	}
	if uid, ok := in.GetUID(); ok {
		parts = append(parts, fmt.Sprintf("uid=%d", uid))
	}
	if gid, ok := in.GetGID(); ok {
		parts = append(parts, fmt.Sprintf("gid=%d", gid))
	}
	if fh, ok := in.GetFh(); ok {
		parts = append(parts, fmt.Sprintf("fh=%d", fh))
	}
	if atime, ok := in.GetATime(); ok {
		parts = append(parts, fmt.Sprintf("atime=%s", trunc(atime.String())))
	}
	if mtime, ok := in.GetMTime(); ok {
		parts = append(parts, fmt.Sprintf("mtime=%s", trunc(mtime.String())))
	}
	if ctime, ok := in.GetCTime(); ok {
		parts = append(parts, fmt.Sprintf("ctime=%s", trunc(ctime.String())))
	}
	if len(parts) == 0 {
		return "{}"
	}
	return "{" + strings.Join(parts, " ") + "}"
}

// BangFileNode represents the ops on a file
type BangFileNode struct {
	fs.Inode
}

// Verify interface compliance
var _ = (fs.NodeGetattrer)((*BangFileNode)(nil))
var _ = (fs.NodeSetattrer)((*BangFileNode)(nil))
var _ = (fs.NodeOpener)((*BangFileNode)(nil))

// GetAttr returns the attributes of a file or directory
func (bf *BangFileNode) Getattr(ctx context.Context, _ fs.FileHandle, out_attr *fuse.AttrOut) syscall.Errno {
	inum := bf.Inode.StableAttr().Ino
	op := bangutil.GetTracer().Op("Getattr", inum, "")

	meta, _, err := kv.Metadata(inum)
	if err != nil {
		op.Error(err)
		return syscall.EIO
	}
	op.SetName(meta.Name)

	MetadataToFuseAttr(inum, meta, &out_attr.Attr)
	op.Done()
	return 0
}

// GetAttr returns the attributes of a file or directory
func (bf *BangFileNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	inum := bf.Inode.StableAttr().Ino
	op := bangutil.GetTracer().Op(fmt.Sprintf("Setattr (%v) (fh: %v)", debugSetAttrIn(in), fh), inum, "")

	// Read the backend metadata
	meta, fvclock, err := kv.Metadata(inum)
	if err != nil {
		op.Error(err)
		return syscall.EIO
	}
	// if IsDir(meta) {
	// 	op.Debug("is a dir") // Just debugging
	// }
	op.SetName(meta.Name)

	// Update the metadata
	// Note: fh (file handle) is informational — the kernel may pass it along
	// with chmod/truncate calls on open files. We ignore it.
	_, setuid := in.GetUID()
	_, setgid := in.GetGID()
	if setuid || setgid {
		op.Debugf("NOT SUPPORTED YET: uid: %v, gid: %v", setuid, setgid)
		return syscall.ENOTSUP
	}
	var stale_chunk_keys []uint64
	sz, setsize := in.GetSize()
	if setsize {
		if !IsFile(meta) {
			op.Debug("truncate on non-file")
			return syscall.ENOTSUP
		}
		op.Debugf("truncate to size: %d (was %d)", sz, meta.Size)
		if sz > meta.Size {
			// Extending file not supported yet
			op.Debug("extending not supported")
			return syscall.ENOTSUP
		}
		chkrefs := meta.Chunks
		// Walk chunks to find which chunk contains the new EOF
		var cumsz uint64
		keep := 0
		for keep < len(chkrefs) {
			cumsz += uint64(chkrefs[keep].Size)
			keep++
			if cumsz >= sz {
				break
			}
		}
		// Collect chunks beyond the new EOF for deferred deletion
		for i := keep; i < len(chkrefs); i++ {
			stale_chunk_keys = append(stale_chunk_keys, chkrefs[i].Hash)
		}
		// Truncate the last kept chunk if the new size falls mid-chunk
		if sz == 0 {
			meta.Chunks = nil
		} else if keep > 0 {
			meta.Chunks = chkrefs[:keep]
			// The last kept chunk may need to be shortened
			last_idx := keep - 1
			chunk_end := cumsz
			chunk_start := chunk_end - uint64(chkrefs[last_idx].Size)
			new_chunk_size := uint32(sz - chunk_start)
			if new_chunk_size < chkrefs[last_idx].Size {
				// Read the chunk, truncate it, write it back
				data, err := kv.Chunk(chkrefs[last_idx].Hash)
				if err != nil {
					op.Errorf("reading chunk %d for truncate: %v", last_idx, err)
					return syscall.EIO
				}
				truncated := data[:new_chunk_size]
				new_chunk_key := chunkidgen.NextId()
				if err := kv.PutChunk(new_chunk_key, truncated); err != nil {
					op.Errorf("writing truncated chunk: %v", err)
					return syscall.EIO
				}
				stale_chunk_keys = append(stale_chunk_keys, chkrefs[last_idx].Hash)
				meta.Chunks[last_idx].Hash = new_chunk_key
				meta.Chunks[last_idx].Size = new_chunk_size
			}
		}
		meta.Size = sz
	}
	mode, setmode := in.GetMode()
	if setmode {
		// Preserve file type bits (S_IFDIR, S_IFREG, etc.), update permission bits
		meta.Mode = (meta.Mode & syscall.S_IFMT) | (mode & ^uint32(syscall.S_IFMT))
	}
	mtime, setmtime := in.GetMTime()
	if setmtime {
		meta.MtimeNs = int64(mtime.UnixNano())
	}
	atime, setatime := in.GetATime()
	if setatime {
		meta.AtimeNs = int64(atime.UnixNano())
	}

	// Write the data back to the backend
	// NOTE: on vclock conflict, the newly-written truncated chunk may be orphaned
	_, err = kv.UpdateMetadata(inum, meta, fvclock)
	if err != nil {
		op.Error(err)
		return syscall.EIO
	}

	// Delete stale chunks only after metadata was successfully written
	for _, key := range stale_chunk_keys {
		if err := kv.DeleteChunk(key); err != nil {
			op.Debugf("failed to delete stale chunk %d: %v", key, err)
		}
	}

	// Populate the out fuse.AttrOut struct
	// TODO: check if this is always supposed to be ALL the attributes or just the changed ones
	MetadataToFuseAttr(inum, meta, &out.Attr)

	op.Done()
	return 0
}

// Open opens the file for reading and writing
func (bf *BangFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {

	inum := bf.Inode.StableAttr().Ino
	op := bangutil.GetTracer().Op("Open", inum, "")

	meta, vclock, err := kv.Metadata(inum)
	if err != nil {
		op.Error(err)
		return nil, 0, syscall.EIO
	}
	// if IsDir(meta) {
	// 	op.Debug("is a dir") // Just debugging
	// }
	op.SetName(meta.Name)

	fh := &BangFH{Inum: inum, Metadata: meta, VClock: vclock, Flags: flags}
	fuse_flags := 0 // TODO: set flags

	op.Done()
	return fh, uint32(fuse_flags), 0
}
