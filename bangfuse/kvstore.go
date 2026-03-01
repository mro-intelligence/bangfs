package bangfuse

import (
	"io"

	bangpb "bangfs/proto"
)

type DiskUsageInfo struct {
	TotalChunks uint64
	UsedChunks  uint64
}

type KVStore interface {
	Connect() error
	InitBackend() error
	Close() error
	PutMetadata(key uint64, newMeta *bangpb.InodeMeta) ([]byte, error)
	Metadata(key uint64) (*bangpb.InodeMeta, []byte, error)
	UpdateMetadata(key uint64, newMeta *bangpb.InodeMeta, vclockIn []byte) ([]byte, error)
	DeleteMetadata(key uint64, vclockIn []byte) error
	PutChunk(key uint64, data []byte) error
	Chunk(key uint64) ([]byte, error)
	DeleteChunk(key uint64) error
	WipeBackend(w io.Writer) error
	DiskUsage(chunkSize uint32) (*DiskUsageInfo, error)
}
