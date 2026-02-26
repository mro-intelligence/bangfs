// Utility functions to read/write file metadata to the backend
package bangfuse

import (
	"fmt"
	"strings"
	"syscall"
	"time"

	pb "bangfs/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// MetadataToFuseAttr fills a FUSE Attr struct from BangFS protobuf metadata.
// The inode number is passed separately because it's the KV key, not stored
// in the protobuf value.
func MetadataToFuseAttr(inum uint64, meta *pb.InodeMeta, out *fuse.Attr) {
	out.Ino = inum
	out.Mode = meta.Mode
	out.Uid = meta.Uid
	out.Gid = meta.Gid
	out.Size = meta.Size
	out.Nlink = meta.Nlink
	out.Atime = uint64(meta.AtimeNs / 1e9)
	out.Atimensec = uint32(meta.AtimeNs % 1e9)
	out.Mtime = uint64(meta.MtimeNs / 1e9)
	out.Mtimensec = uint32(meta.MtimeNs % 1e9)
	out.Ctime = uint64(meta.CtimeNs / 1e9)
	out.Ctimensec = uint32(meta.CtimeNs % 1e9)
}

// IsDir returns true if the metadata represents a directory.
func IsDir(meta *pb.InodeMeta) bool {
	return meta.Mode&syscall.S_IFMT == syscall.S_IFDIR
}

// IsFile returns true if the metadata represents a regular file.
func IsFile(meta *pb.InodeMeta) bool {
	return meta.Mode&syscall.S_IFMT == syscall.S_IFREG
}

// DumpMeta returns a human-readable string of all inode metadata fields.
func DumpMeta(inum uint64, meta *pb.InodeMeta) string {
	var b strings.Builder

	fmt.Fprintf(&b, "inode %d %q\n", inum, meta.Name)
	fmt.Fprintf(&b, "  parent:  %d\n", meta.ParentInode)
	fmt.Fprintf(&b, "  mode:    %s (0%o)\n", formatMode(meta.Mode), meta.Mode&0xFFF)
	fmt.Fprintf(&b, "  uid/gid: %d/%d\n", meta.Uid, meta.Gid)
	fmt.Fprintf(&b, "  nlink:   %d\n", meta.Nlink)
	fmt.Fprintf(&b, "  size:    %d\n", meta.Size)
	fmt.Fprintf(&b, "  ctime:   %s\n", nsToTime(meta.CtimeNs))
	fmt.Fprintf(&b, "  mtime:   %s\n", nsToTime(meta.MtimeNs))
	fmt.Fprintf(&b, "  atime:   %s\n", nsToTime(meta.AtimeNs))

	if cl := meta.GetChildEntries(); cl != nil {
		fmt.Fprintf(&b, "  children: %v\n", cl)
	}
	if ch := meta.GetChunks(); ch != nil {
		fmt.Fprintf(&b, "  chunks:  %d entries\n", len(ch))
	}

	return b.String()
}

func formatMode(mode uint32) string {
	var typ string
	switch mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		typ = "d"
	case syscall.S_IFREG:
		typ = "-"
	case syscall.S_IFLNK:
		typ = "l"
	default:
		typ = "?"
	}

	perm := mode & 0777
	rwx := func(bits uint32) string {
		s := ""
		if bits&4 != 0 {
			s += "r"
		} else {
			s += "-"
		}
		if bits&2 != 0 {
			s += "w"
		} else {
			s += "-"
		}
		if bits&1 != 0 {
			s += "x"
		} else {
			s += "-"
		}
		return s
	}
	return typ + rwx(perm>>6) + rwx((perm>>3)&7) + rwx(perm&7)
}

func nsToTime(ns int64) string {
	if ns == 0 {
		return "(unset)"
	}
	return time.Unix(0, ns).Format("2006-01-02 15:04:05.000")
}
