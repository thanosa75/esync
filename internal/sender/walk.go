package sender

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"esync/internal/fault"
	"esync/internal/obs"
	"esync/internal/plan"
)

// walkConfig is the resolved input to walkTree.
type walkConfig struct {
	root           string // absolute, cleaned source root
	skipAbs        string // absolute path to skip (the dest root, when inside source); "" if none
	followSymlinks bool
	oneFileSystem  bool
	hardlinks      bool
}

type walker struct {
	ctx     obs.Ctx
	cfg     walkConfig
	out     chan<- plan.Entry
	rootDev uint64
	visited map[[2]uint64]bool // (dev,ino) of followed symlink dirs, for cycle detection
	skipped uint32             // E6005/E6006/E6007 warn-and-skip count -> SCAN_COMPLETE.skipped_entries
}

// walkTree enumerates the source tree depth-first (ARCHITECTURE §10.1). It hashes
// nothing. It returns the count of entries skipped with a warning, and a fatal
// fault only when the root itself is unusable.
func walkTree(ctx obs.Ctx, cfg walkConfig, out chan<- plan.Entry) (skipped uint32, err error) {
	fi, lerr := os.Lstat(cfg.root)
	if lerr != nil {
		if errors.Is(lerr, fs.ErrNotExist) {
			return 0, fault.Wrap(fault.E1003, "stat source", cfg.root, lerr)
		}
		return 0, fault.Wrap(fault.E1004, "stat source", cfg.root, lerr)
	}
	dev, _, _ := sysStat(fi)
	w := &walker{ctx: ctx, cfg: cfg, out: out, rootDev: dev, visited: map[[2]uint64]bool{}}

	switch {
	case fi.IsDir():
		w.walkDir(cfg.root, "")
	case fi.Mode().IsRegular():
		w.emitFile(fi, filepath.Base(cfg.root))
	case fi.Mode()&os.ModeSymlink != 0:
		w.handleSymlink(fi, cfg.root, filepath.Base(cfg.root))
	default:
		return 0, fault.New(fault.E1004, "stat source", cfg.root, nil)
	}
	return w.skipped, nil
}

func (w *walker) walkDir(abs, rel string) {
	des, err := os.ReadDir(abs)
	if err != nil {
		w.skipped++
		obs.Warn(w.ctx, "source directory unreadable, skipping",
			obs.F("code", string(fault.E6005)), obs.F("path", abs), obs.F("action", fault.E6005.Action()))
		return
	}
	for _, de := range des {
		name := de.Name()
		if name == ".esync" {
			continue
		}
		childAbs := filepath.Join(abs, name)
		childRel := name
		if rel != "" {
			childRel = rel + string(os.PathSeparator) + name
		}
		if w.cfg.skipAbs != "" && childAbs == w.cfg.skipAbs {
			obs.Warn(w.ctx, "destination is inside the source, skipping it",
				obs.F("code", string(fault.E1008)), obs.F("path", childAbs))
			continue
		}

		fi, err := os.Lstat(childAbs)
		if err != nil {
			obs.Debug(w.ctx, "entry vanished during walk", obs.F("path", childAbs))
			continue
		}

		mode := fi.Mode()
		switch {
		case mode&os.ModeSymlink != 0:
			w.handleSymlink(fi, childAbs, childRel)
		case mode.IsDir():
			if w.cfg.oneFileSystem {
				if d, _, _ := sysStat(fi); d != w.rootDev {
					obs.Debug(w.ctx, "not crossing mount point", obs.F("path", childAbs))
					continue
				}
			}
			w.emitDir(fi, childRel)
			w.walkDir(childAbs, childRel)
		case mode.IsRegular():
			w.emitFile(fi, childRel)
		default:
			w.skipped++
			obs.Warn(w.ctx, "unsupported entry type, skipping",
				obs.F("code", string(fault.E6006)), obs.F("path", childAbs), obs.F("action", fault.E6006.Action()))
		}
	}
}

func (w *walker) handleSymlink(fi os.FileInfo, abs, rel string) {
	target, err := os.Readlink(abs)
	if err != nil {
		obs.Debug(w.ctx, "unreadable symlink, skipping", obs.F("path", abs))
		return
	}
	if !w.cfg.followSymlinks {
		dev, ino, _ := sysStat(fi)
		e := plan.Entry{
			RelPath:    []byte(rel),
			Type:       plan.TypeSymlink,
			Mode:       fi.Mode().Perm(),
			Dev:        dev,
			Ino:        ino,
			LinkTarget: []byte(target),
		}
		setMtime(&e, fi)
		w.out <- e
		return
	}

	st, err := os.Stat(abs)
	if err != nil {
		w.skipped++
		obs.Warn(w.ctx, "dangling symlink, skipping",
			obs.F("code", string(fault.E6006)), obs.F("path", abs))
		return
	}
	dev, ino, _ := sysStat(st)
	if st.IsDir() {
		key := [2]uint64{dev, ino}
		if w.visited[key] {
			w.skipped++
			obs.Warn(w.ctx, "symlink cycle, not descending",
				obs.F("code", string(fault.E6007)), obs.F("path", abs), obs.F("action", fault.E6007.Action()))
			return
		}
		w.visited[key] = true
		w.emitDir(st, rel)
		w.walkDir(abs, rel)
		return
	}
	if st.Mode().IsRegular() {
		w.emitFile(st, rel)
	}
}

func (w *walker) emitDir(fi os.FileInfo, rel string) {
	e := plan.Entry{RelPath: []byte(rel), Type: plan.TypeDir, Mode: fi.Mode().Perm()}
	setMtime(&e, fi)
	w.out <- e
}

func (w *walker) emitFile(fi os.FileInfo, rel string) {
	dev, ino, nlink := sysStat(fi)
	e := plan.Entry{
		RelPath: []byte(rel),
		Type:    plan.TypeFile,
		Size:    fi.Size(),
		Mode:    fi.Mode().Perm(),
		Dev:     dev,
		Ino:     ino,
	}
	setMtime(&e, fi)
	if fi.Mode().Perm()&0o111 != 0 {
		e.Flags |= plan.FlagExecutable
	}
	if w.cfg.hardlinks && nlink > 1 {
		e.Flags |= plan.FlagHasHardlinkKey
		e.HardlinkKey = hardlinkKey(dev, ino)
	}
	w.out <- e
}

func setMtime(e *plan.Entry, fi os.FileInfo) {
	mt := fi.ModTime()
	e.MtimeSec = mt.Unix()
	e.MtimeNsec = uint32(mt.Nanosecond())
}
