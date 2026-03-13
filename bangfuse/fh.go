package bangfuse

import (
	"bangfs/bangutil"
	"context"
	"fmt"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	bangpb "bangfs/proto"
)

type BangFH struct {
	fs.FileHandle
	mut      sync.Mutex
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

// writeMeta writes the metadata to KV and updates the vclock
func (f *BangFH) writeMeta(ctx context.Context) error {
	op := bangutil.GetTracer().Op("writeMeta", f.Inum, f.Metadata.Name)
	//op.Debugf("Write metadata for inode %d, vclock: %v", f.Inum, f.VClock)

	new_vclock, err := gKVStore.UpdateMetadata(f.Inum, f.Metadata, f.VClock)
	if err != nil {
		op.Error(err)
		// Don't reload the vclock, since our metadata is still stale
		return err
	}

	f.VClock = new_vclock // Our metadata should be in sync with what was written
	//op.Debugf("Metadata updated for inode %d", f.Inum)
	op.Done()
	return nil
}

// resyncMetadata rereads the metadata in case of concurrent modification
func (f *BangFH) resyncMetadata(ctx context.Context) error {
	op := bangutil.GetTracer().Op("resyncMetadata", f.Inum, f.Metadata.Name)
	//op.Debugf("Resync metadata for inode %d", f.Inum)

	metadata, new_vclock, err := gKVStore.Metadata(f.Inum)
	if err != nil {
		op.Error(err)
		return err
	}

	f.VClock = new_vclock
	f.Metadata = metadata
	//op.Debugf("Metadata resynced for inode %d, new vclcok: %v", f.Inum, f.VClock)
	op.Done()
	return nil
}

// appendAChunk appends a chunk with the given data to the end of the chunks and updates the receiver members
func (f *BangFH) appendAChunk(ctx context.Context, data []byte) error {

	new_chk_key := gChunkidgen.NextId()
	new_chk_sz := uint64(len(data))

	if new_chk_sz > GetChunkSize() {
		return fmt.Errorf("appendAChunk: data size > chunk size (%d>%d)", new_chk_sz, GetChunkSize())
	}

	if err := gKVStore.PutChunk(new_chk_key, data); err != nil {
		return err
	}

	f.Metadata.Chunks = append(f.Metadata.Chunks, &bangpb.ChunkRef{Hash: new_chk_key, Size: new_chk_sz})
	f.Metadata.Size += new_chk_sz
	return nil
}

// modifyChunk writes data at offset within the chunk at w_chkidx
func (f *BangFH) modifyChunk(ctx context.Context, w_chkidx int, w_chkoffset uint64, data []byte) error {
	op := bangutil.GetTracer().Op("modifyChunk", f.Inum, f.Metadata.Name)

	if w_chkidx >= len(f.Metadata.Chunks) {
		return fmt.Errorf("modifyChunk: idx %d >= %d", w_chkidx, len(f.Metadata.Chunks))
	}

	data_len := uint64(len(data))
	if data_len+w_chkoffset > GetChunkSize() {
		return fmt.Errorf("modifyChunk: %d+%d > chunksize %d", data_len, w_chkoffset, GetChunkSize())
	}

	// REVISIT
	meta_chunk_len := f.Metadata.Chunks[w_chkidx].Size
	if w_chkoffset > meta_chunk_len {
		return fmt.Errorf("modifyChunk: offset %d past chunk end %d", w_chkoffset, meta_chunk_len)
	}

	chk_key := f.Metadata.Chunks[w_chkidx].Hash
	chk_data, err := gKVStore.Chunk(chk_key)
	if err != nil {
		return err
	}

	// Reconcile actual chunk length with metadata
	if chunk_len := uint64(len(chk_data)); chunk_len > meta_chunk_len {
		chk_data = chk_data[:meta_chunk_len]
	}
	if chunk_len := uint64(len(chk_data)); chunk_len < meta_chunk_len {
		chk_data = append(chk_data, make([]byte, meta_chunk_len-chunk_len)...)
	}

	if w_chkoffset == meta_chunk_len {
		chk_data = append(chk_data, data...)
		f.Metadata.Size += data_len
		f.Metadata.Chunks[w_chkidx].Size += data_len
		op.Debugf("chk[%d] append %d", w_chkidx, data_len)
	} else if w_chkoffset+data_len > meta_chunk_len {
		pad_len := w_chkoffset + data_len - meta_chunk_len
		chk_data = append(chk_data, make([]byte, pad_len)...)
		copy(chk_data[w_chkoffset:w_chkoffset+data_len], data)
		f.Metadata.Size += pad_len
		f.Metadata.Chunks[w_chkidx].Size += pad_len
		op.Debugf("chk[%d] extend +%d", w_chkidx, pad_len)
	} else {
		copy(chk_data[w_chkoffset:w_chkoffset+data_len], data)
		op.Debugf("chk[%d] overwrite %d@%d", w_chkidx, data_len, w_chkoffset)
	}

	if err := gKVStore.PutChunk(chk_key, chk_data); err != nil {
		return err
	}

	op.Done()
	return nil
}

// Write writes data at off_in, handling append/overwrite/extend. Returns bytes written.
func (f *BangFH) Write(ctx context.Context, data []byte, off_in int64) (uint32, syscall.Errno) {
	f.mut.Lock()
	defer f.mut.Unlock()

	op := bangutil.GetTracer().Op("Write", f.Inum, f.Metadata.Name)

	// REVISIT: resync to catch Setattr changes; could track FHs in BangFile instead
	if err := f.resyncMetadata(ctx); err != nil {
		op.Error(fmt.Errorf("resyncMetadata: %v", err))
		return 0, syscall.EIO
	}

	if off_in < 0 {
		op.Error(fmt.Errorf("negative offset"))
		return 0, syscall.ENOTSUP
	}

	w_off_st := uint64(off_in)
	if f.Flags&syscall.O_APPEND != 0 {
		w_off_st = f.Metadata.Size
	}
	w_off := w_off_st
	data_off := uint64(0)
	data_len := uint64(len(data))

	for w_off-w_off_st < data_len {
		w_chkidx := int(w_off / GetChunkSize())
		w_chkoffset := w_off % GetChunkSize()
		w_len := min(GetChunkSize()-w_chkoffset, data_len-data_off)

		op.Debugf("chk[%d] off=%d len=%d written=%d/%d", w_chkidx, w_chkoffset, w_len, w_off-w_off_st, data_len)

		if w_chkidx == len(f.Metadata.Chunks) {
			if err := f.appendAChunk(ctx, data[data_off:data_off+w_len]); err != nil {
				op.Error(err)
				return uint32(w_off - w_off_st), syscall.EIO
			}
			data_off += w_len
			w_off += w_len
			continue
		}
		if w_chkidx < len(f.Metadata.Chunks) {
			err := f.modifyChunk(ctx, w_chkidx, w_chkoffset, data[data_off:data_off+w_len])
			if err != nil {
				op.Error(err)
				return uint32(w_off - w_off_st), syscall.EIO
			}
			data_off += w_len
			w_off += w_len
			continue
		}
		op.Errorf("chk idx %d out of range %d", w_chkidx, len(f.Metadata.Chunks))
		return uint32(w_off - w_off_st), syscall.EIO
	}

	op.Debugf("wrote %d bytes @%d", w_off-w_off_st, w_off_st)

	err := f.writeMeta(ctx)
	if err != nil {
		op.Error(err)
		return uint32(w_off - w_off_st), syscall.EIO
	}

	op.Done()
	return uint32(len(data)), 0
}

// readInto reads len bytes from the given chunk, offset and appends it to data or returns an error
func (f *BangFH) readInto(_ context.Context, chkidx int, off uint64, read_len uint64, out_data *[]byte) error {
	op := bangutil.GetTracer().Op("readInto", f.Inum, "")
	op.Debugf("readInto: chkidx: %d off: %d read_len:%d", chkidx, off, read_len)

	chks := f.Metadata.Chunks
	if chkidx >= len(chks) || chkidx < 0 {
		err := fmt.Errorf("chunk index %d out of range (%d chunks)", chkidx, len(chks))
		op.Error(err)
		return err
	}
	meta_chk_sz := chks[chkidx].Size
	if off+read_len > meta_chk_sz {
		err := fmt.Errorf("read too long: off+real_len: %v  meta_chk_sz: %v", off+read_len, meta_chk_sz)
		op.Error(err)
		return err
	}
	key := chks[chkidx].Hash
	chk_data, err := gKVStore.Chunk(key)
	if err != nil {
		op.Error(err)
		return err
	}
	chk_len := uint64(len(chk_data))
	if off+read_len > chk_len {
		err := fmt.Errorf("read too long for retrieved chunk: %v > %v", off+read_len, chk_len)
		op.Error(err)
	}
	*out_data = append(*out_data, chk_data[off:off+read_len]...)
	op.Done()
	return nil
}

// Read reads from the file and copies the result to data
func (f *BangFH) Read(ctx context.Context, dest []byte, off_in int64) (fuse.ReadResult, syscall.Errno) {
	op := bangutil.GetTracer().Op("Read", f.Inum, f.Metadata.Name)
	//op.Debugf("Read up to %d bytes at offset %d", len(dest), off)

	//REVISIT: re sync metadata
	//REVISIT: 3 different int types
	if off_in < 0 {
		op.Errorf("negative offset not supported: off_in: %v", off_in)
		return fuse.ReadResultData(nil), syscall.ENOTSUP
	}
	file_off := uint64(off_in)
	file_len := f.Metadata.Size

	if file_off >= file_len {
		//op.Debugf("offset exceeds file size (%d<%d)", len(dest), off)
		op.Done()
		return fuse.ReadResultData(nil), 0
	}

	out_len := uint64(len(dest))
	out_len = min(out_len, file_len-file_off)
	out_buf := make([]byte, 0)

	for uint64(len(out_buf)) < out_len {
		r_off := file_off + uint64(len(out_buf))
		r_chkidx := int(r_off / GetChunkSize())
		r_chkoffset := r_off % GetChunkSize()
		r_len := min(GetChunkSize()-r_chkoffset, out_len-uint64(len(out_buf)))
		if err := f.readInto(ctx, r_chkidx, r_chkoffset, r_len, &out_buf); err != nil {
			op.Errorf("readInto failed: %v", err)
			return fuse.ReadResultData(out_buf), syscall.EIO
		}
	}

	//op.Debugf("Read returning %d bytes", len(buf))
	op.Done()
	return fuse.ReadResultData(out_buf), 0
}
