//go:build unix

package sender

import (
	"io/fs"
	"syscall"
)

// sysStat pulls the device, inode and link count from a stat result. The
// conversions cover the type differences between linux and darwin.
func sysStat(fi fs.FileInfo) (dev, ino, nlink uint64) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, 1
	}
	return uint64(st.Dev), uint64(st.Ino), uint64(st.Nlink)
}

// hardlinkKey folds (dev, ino) into the u64 the wire manifest carries. Entries
// sharing an inode fold to the same value.
func hardlinkKey(dev, ino uint64) uint64 {
	const prime = 1099511628211
	return (dev*prime ^ ino) * prime
}
