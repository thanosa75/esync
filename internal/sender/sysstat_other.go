//go:build !unix

package sender

import "io/fs"

func sysStat(fi fs.FileInfo) (dev, ino, nlink uint64) { return 0, 0, 1 }

func hardlinkKey(dev, ino uint64) uint64 { return 0 }
