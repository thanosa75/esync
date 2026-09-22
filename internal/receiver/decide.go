package receiver

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"esync/internal/digest"
	"esync/internal/fault"
	"esync/internal/fsx"
	"esync/internal/obs"
	"esync/internal/wire"
)

const (
	entryTypeFile    = 0
	entryTypeDir     = 1
	entryTypeSymlink = 2
)

// fileMeta is what fetch needs to request, verify, and publish one file. It is
// produced by the decide phase and consumed by the data-channel workers.
type fileMeta struct {
	rel      string
	size     int64
	mode     os.FileMode
	mtime    time.Time
	digest   []byte // manifest content digest; empty for --quick / zero-length
	hlKey    uint64
	hasHLKey bool
}

type metaStore struct {
	mu sync.Mutex
	m  map[uint64]fileMeta
}

func newMetaStore() *metaStore { return &metaStore{m: make(map[uint64]fileMeta)} }

func (s *metaStore) set(id uint64, fm fileMeta) {
	s.mu.Lock()
	s.m[id] = fm
	s.mu.Unlock()
}

func (s *metaStore) get(id uint64) (fileMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fm, ok := s.m[id]
	return fm, ok
}

// pendingLink is one secondary waiting for its primary's key to be
// materialised. fileID (in addition to rel) lets the R-16 fallback path look
// the secondary's own fileMeta up in metaStore and enqueue a content fetch if
// the hardlink itself cannot be made (§12.4).
type pendingLink struct {
	fileID uint64
	rel    string
}

// hardlinkMap maps a hardlink_key to the first destination path materialised for
// it (ARCHITECTURE §12.4). fileKey records which needed file is the pending
// primary for a key so fetch can register the path once it publishes.
type hardlinkMap struct {
	mu        sync.Mutex
	keyToPath map[uint64]string        // key -> first materialised destination path
	fileKey   map[uint64]uint64        // primary fileID -> key
	seen      map[uint64]bool          // key has a primary assigned this session
	pending   map[uint64][]pendingLink // key -> secondaries awaiting the primary
}

func newHardlinkMap() *hardlinkMap {
	return &hardlinkMap{
		keyToPath: make(map[uint64]string),
		fileKey:   make(map[uint64]uint64),
		seen:      make(map[uint64]bool),
		pending:   make(map[uint64][]pendingLink),
	}
}

func (h *hardlinkMap) pathFor(key uint64) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	p, ok := h.keyToPath[key]
	return p, ok
}

// claimSecondary records fileID/rel as a link to be made once the primary for
// key is materialised, when a primary has already been assigned. It returns
// false when this is the first sighting of key (the caller then becomes the
// primary).
func (h *hardlinkMap) claimSecondary(key, fileID uint64, rel string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.seen[key] {
		return false
	}
	if p, ok := h.keyToPath[key]; ok {
		_ = p // primary already on disk; caller links directly via pathFor
		return false
	}
	h.pending[key] = append(h.pending[key], pendingLink{fileID: fileID, rel: rel})
	return true
}

func (h *hardlinkMap) markPrimary(fileID, key uint64) {
	h.mu.Lock()
	h.seen[key] = true
	h.fileKey[fileID] = key
	h.mu.Unlock()
}

// clear abandons a key whose link() failed: this entry's content is fetched
// normally instead (§12.4). It retires the key completely — path, seen bit and
// any still-parked secondaries — and returns those secondaries so the caller
// can fall them back to content too.
//
// Dropping only keyToPath, which the first version of this did, silently lost
// files: with the seen bit left set and the path gone, claimSecondary parked
// every later entry sharing the key on a primary that had already published
// and would never call registerMaterialised again. Three or more links to one
// inode plus a single EMLINK/EEXIST was enough to leave a hole in the
// destination on an exit-0 run — the R-16 symptom this fix exists to remove.
func (h *hardlinkMap) clear(key uint64) []pendingLink {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.keyToPath, key)
	delete(h.seen, key)
	orphans := h.pending[key]
	delete(h.pending, key)
	return orphans
}

// registerMaterialised is called once fileID's content is known-correct at rel
// — either fetch publishing it, or decide finding it already resolved by a
// SKIP (R-16) — and returns the secondaries that were waiting on this file's
// key so the caller can hardlink them to rel.
// abandon retires fileID's key because fileID itself will never be
// materialised — its content failed permanently after every retry. It returns
// the secondaries that were parked on that key so the caller can account for
// them, because nothing else ever will: registerMaterialised is only reached
// from a successful publish, so without this they stay unlinked, unfetched and
// — worse — uncounted, disappearing from a run whose summary says nothing
// about them.
func (h *hardlinkMap) abandon(fileID uint64) []pendingLink {
	h.mu.Lock()
	defer h.mu.Unlock()
	key, ok := h.fileKey[fileID]
	if !ok {
		return nil
	}
	orphans := h.pending[key]
	delete(h.pending, key)
	delete(h.keyToPath, key)
	delete(h.seen, key)
	return orphans
}

func (h *hardlinkMap) registerMaterialised(fileID uint64, rel string) []pendingLink {
	h.mu.Lock()
	defer h.mu.Unlock()
	key, ok := h.fileKey[fileID]
	if !ok {
		return nil
	}
	if _, done := h.keyToPath[key]; !done {
		h.keyToPath[key] = rel
	}
	waiting := h.pending[key]
	delete(h.pending, key)
	return waiting
}

type dirRec struct {
	mode  os.FileMode
	mtime time.Time
}

// decider runs the §11.2 decision table for one group at a time, on the bounded
// decide-worker pool. One GROUP_DECISION is emitted per group (INV-1), followed
// by CREDIT{+1} (T-PROTO-08).
type decider struct {
	octx     obs.Ctx
	cfg      Config
	dest     *fsx.Dest
	q        *needQueue
	guard    *spaceGuard
	algo     digest.Algo
	destPlat fsx.Platform
	cache    *digest.Cache
	dryRun   bool

	colMu      sync.Mutex
	collisions *fsx.Collisions

	hl    *hardlinkMap
	meta  *metaStore
	prior map[uint64]fsx.Record

	dirMu   sync.Mutex
	dirMeta map[string]dirRec

	send func(wire.Message) error
	sync func() error // journal sync at the group boundary

	counters *obs.Counters

	// Running totals of needed content, grown one group at a time; nil in tests
	// that exercise decideGroup directly.
	neededBytes *atomic.Int64
	neededFiles *atomic.Int64
}

// decideGroup evaluates every entry in gm, materialises directories, symlinks
// and hard links, enqueues the needed files largest-first, then sends exactly
// one GROUP_DECISION and one CREDIT.
func (d *decider) decideGroup(ctx context.Context, gm *wire.GroupManifest) error {
	end := obs.Start(d.octx, "group.decide")

	var needed []uint16
	var rejected []wire.RejectedEntry
	var items []needItem
	var neededBytes int64
	var skipped uint16

	for i := range gm.Entries {
		e := gm.Entries[i]
		idx := uint16(i)
		fileID := gm.FirstFileID + uint64(i)
		relSlash := string(e.Path)

		if err := fsx.ValidatePath(e.Path, d.destPlat); err != nil {
			rejected = append(rejected, wire.RejectedEntry{Index: idx, ErrorCode: numericCode(fault.GetCode(err))})
			d.counters.FilesRejected.Add(1)
			if e.EntryType == entryTypeSymlink {
				d.counters.SymlinksRejected.Add(1)
			}
			obs.LogFault(d.octx, err)
			continue
		}
		rel := filepath.FromSlash(relSlash)

		d.colMu.Lock()
		cErr := d.collisions.Add(relSlash)
		d.colMu.Unlock()
		if cErr != nil {
			rejected = append(rejected, wire.RejectedEntry{Index: idx, ErrorCode: numericCode(fault.GetCode(cErr))})
			d.counters.FilesRejected.Add(1)
			obs.LogFault(d.octx, cErr)
			continue
		}

		if _, ok := d.prior[fileID]; ok {
			// Completed in a previous session and the manifest digest matches.
			skipped++
			d.counters.FilesSkipped.Add(1)
			continue
		}

		switch e.EntryType {
		case entryTypeDir:
			d.decideDir(rel, e)
		case entryTypeSymlink:
			d.decideSymlink(rel, e)
			skipped++
		default:
			fm := fileMeta{
				rel:      rel,
				size:     int64(e.Size),
				mode:     os.FileMode(e.Mode).Perm(),
				mtime:    time.Unix(e.MtimeSec, int64(e.MtimeNsec)),
				digest:   append([]byte(nil), e.Digest...),
				hlKey:    e.HardlinkKey,
				hasHLKey: e.Flags&wire.ManifestFlagHasHardlinkKey != 0,
			}
			if fm.hasHLKey {
				// Needed even if this entry turns out not to need fetching: a
				// hardlink secondary parked on this key may have to be
				// fetched as ordinary content later (R-16 fallback), and that
				// needs this file's size/mode/mtime/digest.
				d.meta.set(fileID, fm)
			}
			need := d.decideFile(ctx, gm.GroupID, rel, fileID, e)
			if !need {
				skipped++
				d.counters.FilesSkipped.Add(1)
				continue
			}
			if !fm.hasHLKey {
				d.meta.set(fileID, fm)
			}
			needed = append(needed, idx)
			neededBytes += int64(e.Size)
			items = append(items, needItem{fileID: fileID, groupID: gm.GroupID, size: int64(e.Size), rel: rel})
		}
	}

	sort.Slice(needed, func(i, j int) bool { return needed[i] < needed[j] })

	if d.neededBytes != nil {
		d.neededBytes.Add(neededBytes)
		d.neededFiles.Add(int64(len(needed)))
	}

	// GROUP_DECISION first so the sender can begin serving this group's requests.
	if err := d.send(&wire.GroupDecision{
		GroupID:      gm.GroupID,
		Needed:       needed,
		SkippedCount: skipped,
		Rejected:     rejected,
	}); err != nil {
		end("error")
		return err
	}
	if d.sync != nil {
		if err := d.sync(); err != nil {
			end("error")
			return err
		}
	}

	// Free-space guard (§12.7): evaluated before any content is written.
	if neededBytes > 0 {
		warn, ferr := d.guard.reserve(neededBytes)
		if warn {
			obs.Warn(d.octx, "destination free space is low", obs.F("group", gm.GroupID))
		}
		if ferr != nil {
			end("error")
			return ferr
		}
	}

	// Enqueue largest-first; blocks here under backpressure, which withholds the
	// CREDIT below and stalls the sender's manifest publisher (§13.5).
	if !d.dryRun {
		if err := d.q.pushGroup(ctx, items); err != nil {
			end("error")
			if errors.Is(err, errQueueClosed) || ctx.Err() != nil {
				return nil
			}
			return err
		}
	}

	if err := d.send(&wire.Credit{AdditionalGroups: 1}); err != nil {
		end("error")
		return err
	}
	end("ok", obs.F("group", gm.GroupID), obs.F("needed", len(needed)), obs.F("skipped", int(skipped)))
	return nil
}

func (d *decider) decideDir(rel string, e wire.ManifestEntry) {
	d.counters.Dirs.Add(1)
	if rel != "" && rel != "." {
		d.dirMu.Lock()
		d.dirMeta[rel] = dirRec{mode: os.FileMode(e.Mode).Perm(), mtime: time.Unix(e.MtimeSec, int64(e.MtimeNsec))}
		d.dirMu.Unlock()
	}
	if d.dryRun {
		return
	}
	// Create the directory traversable/writable now; the manifest mode (which may
	// be read-only) is stamped at the end by applyDirMeta once every child exists
	// (ARCHITECTURE §12.3).
	if err := d.dest.EnsureDir(rel, 0o755); err != nil {
		obs.LogFault(d.octx, err)
	}
}

func (d *decider) decideSymlink(rel string, e wire.ManifestEntry) {
	target := string(e.LinkTarget)
	if d.dryRun {
		d.counters.Symlinks.Add(1)
		return
	}
	// symlink vs symlink, same target => skip.
	if fi, err := d.dest.Root().Lstat(rel); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		if cur, rerr := d.dest.Root().Readlink(rel); rerr == nil && cur == target {
			return
		}
	}
	if err := d.dest.MakeSymlink(rel, target, d.cfg.AllowUnsafeLinks); err != nil {
		obs.LogFault(d.octx, err)
		if fault.GetCode(err) == fault.E7011 {
			d.counters.SymlinksRejected.Add(1)
		}
		return
	}
	d.counters.Symlinks.Add(1)
}

// decideFile applies the §11.2 table. It returns true when the file's content
// must be fetched.
func (d *decider) decideFile(ctx context.Context, groupID uint32, rel string, fileID uint64, e wire.ManifestEntry) bool {
	isPrimary := false
	if e.Flags&wire.ManifestFlagHasHardlinkKey != 0 {
		key := e.HardlinkKey
		if p, ok := d.hl.pathFor(key); ok {
			if d.dryRun {
				return false
			}
			if err := d.dest.MakeHardlink(rel, p); err == nil {
				return false
			} else {
				// §12.4: fall back to fetching the content. This entry takes
				// over as the key's primary, so later entries sharing it link
				// to this copy instead of parking forever on the old one.
				obs.LogFault(d.octx, err)
				for _, orphan := range d.hl.clear(key) {
					d.fallbackFetch(groupID, orphan.fileID, orphan.rel)
				}
				d.hl.markPrimary(fileID, key)
				isPrimary = true
			}
		} else if d.hl.claimSecondary(key, fileID, rel) {
			// A primary is pending; fetch will create this link once it lands,
			// or decide will materialise it here if the primary turns out to
			// be a SKIP rather than a publish (R-16, see below).
			return false
		} else {
			d.hl.markPrimary(fileID, key)
			isPrimary = true
		}
	}

	need := d.decideFileContent(rel, e)

	// R-16: a hardlink primary resolved by SKIP never publishes, so fetch's
	// registerMaterialised (called post-publish) is never reached and any
	// secondary parked on this key would otherwise stay absent from the
	// destination forever. Materialise them now against the already-present
	// rel.
	if isPrimary && !need && !d.dryRun {
		d.linkOrFallback(groupID, rel, d.hl.registerMaterialised(fileID, rel))
	}
	return need
}

// decideFileContent is the §11.2 existence/size/mtime/digest comparison. It
// returns true when the file's content must be fetched.
func (d *decider) decideFileContent(rel string, e wire.ManifestEntry) bool {
	fi, err := d.dest.Root().Lstat(rel)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return true
	case err != nil:
		obs.Warn(d.octx, "destination file unreadable, will refetch", obs.F("path", rel), obs.F("err", err.Error()))
		return true
	}
	if !fi.Mode().IsRegular() {
		return true // directory or symlink where a file is expected
	}
	if fi.Size() != int64(e.Size) {
		return true
	}
	if d.cfg.Quick && mtimeEqual(fi, e) {
		return false
	}
	if len(e.Digest) == 0 {
		return e.Size != 0
	}
	local, herr := digest.HashFile(d.octx, filepath.Join(d.dest.Dir(), rel), d.algo, d.cache)
	if herr != nil {
		obs.Warn(d.octx, "hashing destination file failed, will refetch", obs.F("path", rel), obs.F("err", herr.Error()))
		return true
	}
	return !bytes.Equal(local, e.Digest)
}

// linkOrFallback hardlinks each pending secondary to primaryRel (already
// known-correct content). If MakeHardlink itself fails, it falls back to
// fetching that secondary's content as an ordinary needed file (ARCHITECTURE
// §12.4) instead of leaving it silently absent from the destination (R-16).
func (d *decider) linkOrFallback(groupID uint32, primaryRel string, pending []pendingLink) {
	for _, sec := range pending {
		if err := d.dest.MakeHardlink(sec.rel, primaryRel); err != nil {
			obs.LogFault(d.octx, err)
			d.fallbackFetch(groupID, sec.fileID, sec.rel)
		}
	}
}

// fallbackFetch enqueues fileID/rel as an ordinary needed file when a hardlink
// secondary could not be linked (§12.4 / R-16). fileID's group has already
// sent its GROUP_DECISION (this runs well after that, from a different
// group's decideFile), so the item is pushed straight into the need queue
// under groupID (the group currently being decided) rather than folded into
// any GROUP_DECISION.Needed list.
func (d *decider) fallbackFetch(groupID uint32, fileID uint64, rel string) {
	d.fallbackDeps().fetchInstead(groupID, fileID, rel)
}

func (d *decider) fallbackDeps() hlFallbackDeps {
	return hlFallbackDeps{
		octx: d.octx, meta: d.meta, guard: d.guard, q: d.q,
		counters: d.counters, neededBytes: d.neededBytes, neededFiles: d.neededFiles,
	}
}

func mtimeEqual(fi fs.FileInfo, e wire.ManifestEntry) bool {
	mt := fi.ModTime()
	return mt.Unix() == e.MtimeSec && int64(mt.Nanosecond()) == int64(e.MtimeNsec)
}

func numericCode(c fault.Code) uint16 {
	n, _ := strconv.Atoi(strings.TrimPrefix(string(c), "E"))
	return uint16(n)
}
