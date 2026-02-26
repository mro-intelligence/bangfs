// Package util provides utility functions for bangfs
package bangutil

import (
	"hash/fnv"
)

// FNV32 computes a 32-bit FNV hash of the data
func FNV32(data []byte) uint32{
	h := fnv.New32a()
	h.Write(data)
	return h.Sum32()
}

// FNV64 computes a 64-bit FNV-1a hash of the data
func FNV64(data []byte) uint64 {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum64()
}

// FNV64Bytes computes a 64-bit FNV-1a hash and returns it as bytes
func FNV64Bytes(data []byte) []byte {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum(nil)
}

// FNV128Bytes computes a 128-bit FNV-1a hash and returns it as bytes
func FNV128Bytes(data []byte) []byte {
	h := fnv.New128a()
	h.Write(data)
	return h.Sum(nil)
}

// HashChunk computes the hash for a chunk of data
// Uses FNV-1a 64-bit for speed (non-cryptographic)
func HashChunk(data []byte) []byte {
	return FNV64Bytes(data)
}
