package bangfuse

import (
	"bangfs/bangutil"
	"context"
	"fmt"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	bangpb "bangfs/proto"
)

type BangFH struct {
	fs.FileHandle
	Flags    uint32
	Inum     uint64
	Metadata *bangpb.InodeMeta
	VClock   []byte
}

func (f *BangFH) String() string {
	name := ""
	if f.Metadata != nil {
		name = f.Metadata.Name
	}
	return fmt.Sprintf("FH{inum=%d name=%q flags=0x%x}", f.Inum, name, f.Flags)
}

var _ = (fs.FileWriter)((*BangFH)(nil))
var _ = (fs.FileReader)((*BangFH)(nil))

//var _ = (fs.File)

// replaceChunk replaces a chunk in the file with new data
func (f *BangFH) replaceChunk(ctx context.Context, idx int, data []byte) error {
	op := bangutil.GetTracer().Op("replaceChunk", f.Inum, f.Metadata.Name)

	chks := f.Metadata.Chunks
	num_chunks := len(chks)
	if idx >= num_chunks {
		op.Error(fmt.Errorf("chunk index %d out of range (%d chunks)", idx, num_chunks))
		return syscall.EIO
	}

	key := gChunkidgen.NextId()
	err := gKVStore.PutChunk(key, data)
	if err != nil {
		op.Error(err)
		return err
	}

	chks[idx].Hash = key
	chks[idx].Size = uint32(len(data))

	f.Metadata.Chunks = chks
	op.Done()
	return nil
}

// readChunk returns the content of a chunk at index idx
func (f *BangFH) readChunk(ctx context.Context, idx int) ([]byte, error) {
	op := bangutil.GetTracer().Op("BangFH.readChunk", f.Inum, f.Metadata.Name)

	chks := f.Metadata.Chunks
	if idx >= len(chks) || idx < 0 {
		err := fmt.Errorf("chunk index %d out of range (%d chunks)", idx, len(chks))
		op.Error(err)
		return nil, err
	}
	key := chks[idx].Hash
	data, err := gKVStore.Chunk(key)
	if err != nil {
		op.Error(err)
		return nil, err
	}
	op.Done()
	return data, nil
}

// appendChunk appends a new chunk to the file but defers writing metadata
func (f *BangFH) appendChunk(ctx context.Context, data []byte) error {
	op := bangutil.GetTracer().Op("appendChunk", f.Inum, f.Metadata.Name)

	chunkrefs := f.Metadata.Chunks

	key := gChunkidgen.NextId()
	err := gKVStore.PutChunk(key, data)
	if err != nil {
		op.Error(err)
		return err
	}
	// TODO: decide if to undo the metadata or resync it if this fails
	chunkrefs = append(chunkrefs, &bangpb.ChunkRef{Hash: key, Size: uint32(len(data))})
	f.Metadata.Chunks = chunkrefs

	op.Done()
	return nil
}

// writeMeta writes the metadata to KV and updates the vclock
func (f *BangFH) writeMeta(ctx context.Context) error {
	op := bangutil.GetTracer().Op("writeMeta", f.Inum, f.Metadata.Name)
	op.Debugf("Write metadata for inode %d, vclock: %v", f.Inum, f.VClock)

	new_vclock, err := gKVStore.UpdateMetadata(f.Inum, f.Metadata, f.VClock)
	if err != nil {
		op.Error(err)
		// Don't reload the vclock, since our metadata is still stale
		return err
	}

	f.VClock = new_vclock // Our metadata should be in sync with what was written
	op.Debugf("Metadata updated for inode %d, new vclcok: %v", f.Inum, f.VClock)
	op.Done()
	return nil
}

// resyncMetadata rereads the metadata in case of concurrent modification
func (f *BangFH) resyncMetadata(ctx context.Context) error {
	op := bangutil.GetTracer().Op("resyncMetadata", f.Inum, f.Metadata.Name)
	op.Debugf("Resync metadata for inode %d", f.Inum)

	metadata, new_vclock, err := gKVStore.Metadata(f.Inum)
	if err != nil {
		op.Error(err)
		return err
	}

	f.VClock = new_vclock
	f.Metadata = metadata
	op.Debugf("Metadata resynced for inode %d, new vclcok: %v", f.Inum, f.VClock)
	op.Done()
	return nil
}

// writeAt splices data into the file at the given offset, modifying existing
// chunks and appending new ones as needed.
// All chunks except the last are exactly gChunksize bytes, so we use division
// to index directly instead of walking.
func (f *BangFH) writeAt(ctx context.Context, op *bangutil.TraceOp, data []byte, off int64) syscall.Errno {
	chks := f.Metadata.Chunks
	pos := off    // current file position
	data_pos := 0 // how far into data we've consumed

	for data_pos < len(data) {
		chunk_idx := int(pos / int64(gChunksize))
		offset_in_chunk := int(pos % int64(gChunksize))

		if chunk_idx < len(chks) {
			// Overwrite within an existing chunk
			existing, err := f.readChunk(ctx, chunk_idx)
			if err != nil {
				op.Errorf("readChunk[%d]: %v", chunk_idx, err)
				return syscall.EIO
			}
			// Extend the chunk buffer if the write goes past its current size
			// (can happen on the last chunk which may be shorter than gChunksize)
			if offset_in_chunk+len(data)-data_pos > len(existing) && len(existing) < int(gChunksize) {
				grown := make([]byte, min(int(gChunksize), offset_in_chunk+len(data)-data_pos))
				copy(grown, existing)
				existing = grown
			}
			n := copy(existing[offset_in_chunk:], data[data_pos:])
			data_pos += n
			pos += int64(n)
			if err := f.replaceChunk(ctx, chunk_idx, existing); err != nil {
				op.Errorf("replaceChunk[%d]: %v", chunk_idx, err)
				return syscall.EIO
			}
		} else {
			// Past the last chunk — append new ones
			remaining := len(data) - data_pos
			n := min(uint32(remaining), gChunksize)
			if err := f.appendChunk(ctx, data[data_pos:data_pos+int(n)]); err != nil {
				op.Errorf("appendChunk: %v", err)
				return syscall.EIO
			}
			data_pos += int(n)
			pos += int64(n)
			// appendChunk updates f.Metadata.Chunks, refresh local ref
			chks = f.Metadata.Chunks
		}
	}

	return 0
}

// Write writes to the inode at the given offset and returns the number of bytes written.
// Handles append, overwrite, and write-past-EOF (zero-fill gap).
func (f *BangFH) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	op := bangutil.GetTracer().Op("Write", f.Inum, f.Metadata.Name)
	op.Debugf("Write %d bytes at offset %d to inode %d", len(data), off, f.Inum)

	// Re-read metadata: Setattr (e.g. O_TRUNC truncate) may have changed it.
	// TODO: to save an extra read call we can track filehandles in the BangFile struct.
	if err := f.resyncMetadata(ctx); err != nil {
		op.Error(fmt.Errorf("resyncMetadata: %v", err))
		return 0, syscall.EIO
	}

	filesize := int64(f.Metadata.Size)

	// O_APPEND: force offset to end of file regardless of what the kernel sent
	if f.Flags&syscall.O_APPEND != 0 {
		op.Debugf("O_APPEND: adjusting offset from %d to %d", off, filesize)
		off = filesize
	}

	write_end := off + int64(len(data))

	// If writing past EOF, zero-fill the gap
	if off > filesize {
		gap := make([]byte, off-filesize)
		if errno := f.writeAt(ctx, op, gap, filesize); errno != 0 {
			return 0, errno
		}
		filesize = off
	}

	if errno := f.writeAt(ctx, op, data, off); errno != 0 {
		return 0, errno
	}

	// Update file size
	if write_end > filesize {
		f.Metadata.Size = uint64(write_end)
	}

	if err := f.writeMeta(ctx); err != nil {
		op.Error(fmt.Errorf("syncing metadata (chunks and size): %v", err))
		return 0, syscall.EIO
	}

	op.Debugf("Wrote %d bytes at offset %d (new size: %d)", len(data), off, f.Metadata.Size)
	op.Done()
	return uint32(len(data)), 0
}

// Read reads from the file and copies the result to data
func (f *BangFH) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	op := bangutil.GetTracer().Op("Read", f.Inum, f.Metadata.Name)
	op.Debugf("Read up to %d bytes at offset %d", len(dest), off)

	filesize := int64(f.Metadata.Size)
	if off >= filesize {
		op.Debugf("offset exceeds file size (%d<%d)", len(dest), off)
		op.Done()
		return fuse.ReadResultData(nil), 0
	}

	// Clamp read to file size
	end := off + int64(len(dest))
	if end > filesize {
		end = filesize
	}

	chks := f.Metadata.Chunks
	buf := make([]byte, 0, end-off)

	// Walk chunks, accumulating an offset to find which chunks overlap [off, end)
	var chunk_offset int64
	for i, chk := range chks {
		chunk_end := chunk_offset + int64(chk.Size)

		if chunk_end <= off {
			// This chunk is entirely before the read window
			chunk_offset = chunk_end
			continue
		}
		if chunk_offset >= end {
			// Past the read window
			break
		}

		// This chunk overlaps the read window — fetch it
		data, err := f.readChunk(ctx, i)
		if err != nil {
			op.Errorf("readChunk[%d]: %v", i, err)
			return nil, syscall.EIO
		}

		// Slice within this chunk that overlaps the read window
		slice_start := int64(0)
		if off > chunk_offset {
			slice_start = off - chunk_offset
		}
		slice_end := int64(chk.Size)
		if end < chunk_end {
			slice_end = end - chunk_offset
		}

		buf = append(buf, data[slice_start:slice_end]...)
		chunk_offset = chunk_end
	}

	op.Debugf("Read returning %d bytes", len(buf))
	op.Done()
	return fuse.ReadResultData(buf), 0
}
