package fsx

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"syscall"
	"time"

	"esync/internal/fault"
)

// Dest is confined destination access. Every path operation goes through an
// *os.Root, which rejects any component that resolves outside the root, plus an
// explicit "no symlink anywhere on the parent path" walk (§12.5).
type Dest struct {
	root    *os.Root
	dir     string
	noFsync bool
}

// OpenDest opens dir as a confined root. dir must already exist.
func OpenDest(dir string, noFsync bool) (*Dest, error) {
	r, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fault.New(fault.E1006, "open destination root", dir, err)
	}
	return &Dest{root: r, dir: dir, noFsync: noFsync}, nil
}

// Root exposes the underlying *os.Root for callers that need raw confined access.
func (d *Dest) Root() *os.Root { return d.root }

// Dir is the destination root path.
func (d *Dest) Dir() string { return d.dir }

// Close releases the root handle.
func (d *Dest) Close() error { return d.root.Close() }

func diskCode(err error, dflt fault.Code) fault.Code {
	switch {
	case errors.Is(err, syscall.ENOSPC):
		return fault.E7003
	case errors.Is(err, os.ErrPermission), errors.Is(err, syscall.EACCES):
		return fault.E7002
	default:
		return dflt
	}
}

// checkParentSymlinks walks the parent components of rel and rejects the first
// one that is a symlink with E7013 (§12.5). A missing component ends the walk.
func (d *Dest) checkParentSymlinks(rel string) error {
	comps := strings.Split(rel, "/")
	for i := 1; i < len(comps); i++ {
		parent := strings.Join(comps[:i], "/")
		if parent == "" || parent == "." {
			continue
		}
		fi, err := d.root.Lstat(parent)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return fault.New(fault.E7013, "lstat destination path component", parent, err)
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return fault.New(fault.E7013, "symlink on destination path", parent, nil)
		}
	}
	return nil
}

func (d *Dest) mkParents(rel string) error {
	dir := path.Dir(rel)
	if dir == "." || dir == "" {
		return nil
	}
	if err := d.root.MkdirAll(dir, 0o777); err != nil {
		return fault.New(diskCode(err, fault.E7008), "create parent directory", dir, err)
	}
	return nil
}

// EnsureDir creates relDir (and any missing parents) and applies mode to the
// leaf. Directory mtime is applied separately at group completion (§12.3).
func (d *Dest) EnsureDir(relDir string, mode os.FileMode) error {
	if relDir == "" || relDir == "." {
		return nil
	}
	if err := d.checkParentSymlinks(relDir); err != nil {
		return err
	}
	if err := d.root.MkdirAll(relDir, 0o777); err != nil {
		return fault.New(diskCode(err, fault.E7008), "create directory", relDir, err)
	}
	if err := d.root.Chmod(relDir, mode); err != nil {
		return fault.New(fault.E7006, "apply directory mode", relDir, err)
	}
	return nil
}

func partName(fileID uint64) string {
	return fmt.Sprintf(".esync/parts/%d.part", fileID)
}

// OpenPart creates and opens <root>/.esync/parts/<id>.part for writing (0600).
func (d *Dest) OpenPart(fileID uint64) (*os.File, error) {
	if err := d.root.MkdirAll(".esync/parts", 0o700); err != nil {
		return nil, fault.New(diskCode(err, fault.E7008), "create parts directory", ".esync/parts", err)
	}
	name := partName(fileID)
	f, err := d.root.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fault.New(diskCode(err, fault.E7001), "open part file", name, err)
	}
	return f, nil
}

// PublishPart atomically moves the part file for fileID to finalRel: fsync
// (unless noFsync), chmod, chtimes, then a root-confined rename (§12.2). A rename
// failure is E7005; only a metadata failure after a successful rename is E7006.
func (d *Dest) PublishPart(fileID uint64, finalRel string, mode os.FileMode, mtime time.Time) error {
	part := partName(fileID)

	if !d.noFsync {
		if f, err := d.root.OpenFile(part, os.O_RDONLY, 0); err == nil {
			_ = f.Sync()
			_ = f.Close()
		}
	}
	if err := d.checkParentSymlinks(finalRel); err != nil {
		return err
	}
	if err := d.mkParents(finalRel); err != nil {
		return err
	}

	var metaErr error
	if err := d.root.Chmod(part, mode); err != nil {
		metaErr = err
	}
	if err := d.root.Chtimes(part, time.Time{}, mtime); err != nil {
		metaErr = err
	}
	if err := d.root.Rename(part, finalRel); err != nil {
		return fault.New(diskCode(err, fault.E7005), "atomic publish", finalRel, err)
	}
	if metaErr != nil {
		return fault.New(fault.E7006, "apply file metadata", finalRel, metaErr)
	}
	return nil
}

// ApplyMeta applies mode and mtime to an already-published entry (used for
// directories at group completion). A failure is E7006 (Warn).
func (d *Dest) ApplyMeta(rel string, mode os.FileMode, mtime time.Time) error {
	var e error
	if err := d.root.Chmod(rel, mode); err != nil {
		e = err
	}
	if err := d.root.Chtimes(rel, time.Time{}, mtime); err != nil {
		e = err
	}
	if e != nil {
		return fault.New(fault.E7006, "apply metadata", rel, e)
	}
	return nil
}

// Chown sets ownership; only called under --owner. A failure is E7006 (Warn).
func (d *Dest) Chown(rel string, uid, gid int) error {
	if err := d.root.Lchown(rel, uid, gid); err != nil {
		return fault.New(fault.E7006, "apply ownership", rel, err)
	}
	return nil
}

func absoluteTarget(t string) bool {
	return strings.HasPrefix(t, "/") || strings.HasPrefix(t, `\`) ||
		(len(t) >= 2 && t[1] == ':' && isASCIILetter(t[0]))
}

// MakeSymlink creates relPath → target. Unless allowUnsafeLinks, an absolute
// target or one that resolves outside the destination root is rejected with
// E7011 (§12.5).
func (d *Dest) MakeSymlink(relPath, target string, allowUnsafeLinks bool) error {
	if err := d.checkParentSymlinks(relPath); err != nil {
		return err
	}
	if !allowUnsafeLinks {
		if absoluteTarget(target) {
			return fault.New(fault.E7011, "symlink target is absolute", relPath, nil)
		}
		resolved := path.Join(path.Dir(relPath), target)
		if resolved == ".." || strings.HasPrefix(resolved, "../") {
			return fault.New(fault.E7011, "symlink target escapes destination root", relPath, nil)
		}
	}
	if err := d.mkParents(relPath); err != nil {
		return err
	}
	_ = d.root.Remove(relPath)
	if err := d.root.Symlink(target, relPath); err != nil {
		return fault.New(diskCode(err, fault.E7001), "create symlink", relPath, err)
	}
	return nil
}

// MakeHardlink links relPath to the already-materialised existingRel. Any
// failure is E7009 (Warn): the caller falls back to requesting the content.
func (d *Dest) MakeHardlink(relPath, existingRel string) error {
	if err := d.checkParentSymlinks(relPath); err != nil {
		return err
	}
	if err := d.mkParents(relPath); err != nil {
		return fault.New(fault.E7009, "create parent for hard link", relPath, err)
	}
	_ = d.root.Remove(relPath)
	if err := d.root.Link(existingRel, relPath); err != nil {
		return fault.New(fault.E7009, "create hard link", relPath, err)
	}
	return nil
}
