//go:build unix

package fsx

import (
	"path/filepath"
	"strings"
	"syscall"

	"esync/internal/fault"
)

// FreeBytes reports the bytes available to an unprivileged writer at dest via
// statfs (§12.7). The caller keeps the running requirement and the 64 MiB margin.
func FreeBytes(dest string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dest, &st); err != nil {
		return 0, fault.New(fault.E1006, "statfs destination", dest, err)
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}

// IsInside reports whether child is the same path as, or nested within, parent
// after resolving both to absolute, symlink-free paths (self-copy guard,
// REQ-FS-045 / E1008).
func IsInside(child, parent string) (bool, error) {
	c, err := resolve(child)
	if err != nil {
		return false, fault.New(fault.E1008, "resolve destination path", child, err)
	}
	p, err := resolve(parent)
	if err != nil {
		return false, fault.New(fault.E1008, "resolve source path", parent, err)
	}
	rel, err := filepath.Rel(p, c)
	if err != nil {
		return false, nil
	}
	if rel == "." {
		return true, nil
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)), nil
}

func resolve(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	if r, err := filepath.EvalSymlinks(abs); err == nil {
		return r, nil
	}
	return abs, nil
}
