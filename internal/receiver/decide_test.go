package receiver

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"esync/internal/digest"
	"esync/internal/fault"
	"esync/internal/fsx"
	"esync/internal/obs"
	"esync/internal/wire"
)

func md5sum(t *testing.T, b []byte) []byte {
	t.Helper()
	h, err := digest.New(digest.MD5)
	if err != nil {
		t.Fatal(err)
	}
	h.Write(b)
	return h.Sum(nil)
}

type sendLog struct {
	msgs []wire.Message
}

func (s *sendLog) fn(m wire.Message) error { s.msgs = append(s.msgs, m); return nil }

func (s *sendLog) only(t *testing.T) *wire.GroupDecision {
	t.Helper()
	var gd *wire.GroupDecision
	for _, m := range s.msgs {
		if d, ok := m.(*wire.GroupDecision); ok {
			if gd != nil {
				t.Fatal("more than one GROUP_DECISION emitted (INV-1)")
			}
			gd = d
		}
	}
	if gd == nil {
		t.Fatal("no GROUP_DECISION emitted")
	}
	return gd
}

type testDecider struct {
	dec         *decider
	log         *sendLog
	q           *needQueue
	cnt         *obs.Counters
	dir         string
	neededBytes atomic.Int64
	neededFiles atomic.Int64
}

func newTestDecider(t *testing.T, cfg Config, plat fsx.Platform, prior map[uint64]fsx.Record) *testDecider {
	t.Helper()
	dir := t.TempDir()
	dest, err := fsx.OpenDest(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dest.Close() })
	log := &sendLog{}
	q := newNeedQueue(needQueueDepth)
	cnt := &obs.Counters{}
	if prior == nil {
		prior = map[uint64]fsx.Record{}
	}
	d := &decider{
		octx:       obs.Ctx{},
		cfg:        cfg,
		dest:       dest,
		q:          q,
		guard:      newSpaceGuard(1<<40, spaceMarginBytes),
		algo:       digest.MD5,
		destPlat:   plat,
		collisions: fsx.NewCollisions(plat),
		hl:         newHardlinkMap(),
		meta:       newMetaStore(),
		prior:      prior,
		dirMeta:    map[string]dirRec{},
		send:       log.fn,
		counters:   cnt,
	}
	td := &testDecider{dec: d, log: log, q: q, cnt: cnt, dir: dir}
	d.neededBytes = &td.neededBytes
	d.neededFiles = &td.neededFiles
	return td
}

func fileEntry(path string, data []byte, digestBytes []byte, mtime time.Time) wire.ManifestEntry {
	return wire.ManifestEntry{
		EntryType: entryTypeFile,
		Path:      []byte(path),
		Size:      uint64(len(data)),
		Mode:      0o644,
		MtimeSec:  mtime.Unix(),
		MtimeNsec: uint32(mtime.Nanosecond()),
		Digest:    digestBytes,
	}
}

// T-DEC-01: a file absent at the destination is needed.
func TestDecideNeedsAbsentFile(t *testing.T) {
	td := newTestDecider(t, Config{}.withDefaults(), fsx.PlatformLinux, nil)
	data := []byte("hello world")
	gm := &wire.GroupManifest{GroupID: 0, FirstFileID: 0, Entries: []wire.ManifestEntry{
		fileEntry("a.txt", data, md5sum(t, data), time.Unix(1000, 0)),
	}}
	if err := td.dec.decideGroup(context.Background(), gm); err != nil {
		t.Fatalf("decideGroup: %v", err)
	}
	gd := td.log.only(t)
	if len(gd.Needed) != 1 || gd.Needed[0] != 0 {
		t.Fatalf("Needed = %v, want [0]", gd.Needed)
	}
	it, ok := td.q.pop(context.Background())
	if !ok || it.fileID != 0 || it.size != int64(len(data)) {
		t.Fatalf("queued item = %+v, ok=%v", it, ok)
	}
}

// The decider grows the running needed-bytes/needed-files totals (the progress
// denominator, §15.10) by the needed entries only — a present, identical file
// contributes nothing.
func TestDecideAccumulatesNeededTotals(t *testing.T) {
	td := newTestDecider(t, Config{}.withDefaults(), fsx.PlatformLinux, nil)
	want := []byte("twelve bytes")
	have := []byte("present already")
	if err := os.WriteFile(filepath.Join(td.dir, "have.txt"), have, 0o644); err != nil {
		t.Fatal(err)
	}
	gm := &wire.GroupManifest{Entries: []wire.ManifestEntry{
		fileEntry("want.txt", want, md5sum(t, want), time.Unix(1000, 0)),
		fileEntry("have.txt", have, md5sum(t, have), time.Unix(1000, 0)),
	}}
	if err := td.dec.decideGroup(context.Background(), gm); err != nil {
		t.Fatalf("decideGroup: %v", err)
	}
	if got := td.neededBytes.Load(); got != int64(len(want)) {
		t.Fatalf("neededBytes = %d, want %d", got, len(want))
	}
	if got := td.neededFiles.Load(); got != 1 {
		t.Fatalf("neededFiles = %d, want 1", got)
	}
}

// T-DEC-02: a file already present with matching size and content digest is
// skipped, not requested.
func TestDecideSkipsIdenticalFile(t *testing.T) {
	td := newTestDecider(t, Config{}.withDefaults(), fsx.PlatformLinux, nil)
	data := []byte("unchanged bytes")
	if err := os.WriteFile(filepath.Join(td.dir, "keep.txt"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	gm := &wire.GroupManifest{Entries: []wire.ManifestEntry{
		fileEntry("keep.txt", data, md5sum(t, data), time.Unix(1000, 0)),
	}}
	if err := td.dec.decideGroup(context.Background(), gm); err != nil {
		t.Fatalf("decideGroup: %v", err)
	}
	gd := td.log.only(t)
	if len(gd.Needed) != 0 {
		t.Fatalf("Needed = %v, want none", gd.Needed)
	}
	if gd.SkippedCount != 1 {
		t.Fatalf("SkippedCount = %d, want 1", gd.SkippedCount)
	}
	if td.cnt.FilesSkipped.Load() != 1 {
		t.Fatalf("FilesSkipped counter = %d, want 1", td.cnt.FilesSkipped.Load())
	}
}

// T-DEC-02 (contrapositive): differing content digest forces a refetch.
func TestDecideRefetchOnDigestMismatch(t *testing.T) {
	td := newTestDecider(t, Config{}.withDefaults(), fsx.PlatformLinux, nil)
	local := []byte("old content...")
	remote := []byte("new content!!!") // same length, different bytes
	if err := os.WriteFile(filepath.Join(td.dir, "f.txt"), local, 0o644); err != nil {
		t.Fatal(err)
	}
	gm := &wire.GroupManifest{Entries: []wire.ManifestEntry{
		fileEntry("f.txt", remote, md5sum(t, remote), time.Unix(1000, 0)),
	}}
	if err := td.dec.decideGroup(context.Background(), gm); err != nil {
		t.Fatalf("decideGroup: %v", err)
	}
	if gd := td.log.only(t); len(gd.Needed) != 1 {
		t.Fatalf("Needed = %v, want [0]", gd.Needed)
	}
}

// T-DEC-03: with --quick an equal size and mtime is accepted without hashing,
// even when the content differs.
func TestDecideQuickSkipsOnSizeAndMtime(t *testing.T) {
	cfg := Config{Quick: true}.withDefaults()
	td := newTestDecider(t, cfg, fsx.PlatformLinux, nil)
	local := []byte("AAAAAAAAAA")
	remote := []byte("BBBBBBBBBB")
	p := filepath.Join(td.dir, "q.txt")
	if err := os.WriteFile(p, local, 0o644); err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(1735000000, 12345)
	if err := os.Chtimes(p, mt, mt); err != nil {
		t.Fatal(err)
	}
	gm := &wire.GroupManifest{Entries: []wire.ManifestEntry{
		fileEntry("q.txt", remote, md5sum(t, remote), mt),
	}}
	if err := td.dec.decideGroup(context.Background(), gm); err != nil {
		t.Fatalf("decideGroup: %v", err)
	}
	if gd := td.log.only(t); len(gd.Needed) != 0 {
		t.Fatalf("Needed = %v, want none (quick skip)", gd.Needed)
	}
}

// T-RES-01: a file recorded complete in the resume journal is skipped.
func TestDecideSkipsJournalledFile(t *testing.T) {
	prior := map[uint64]fsx.Record{0: {FileID: 0, Path: "done.txt", Size: 3}}
	td := newTestDecider(t, Config{}.withDefaults(), fsx.PlatformLinux, prior)
	data := []byte("abc")
	gm := &wire.GroupManifest{Entries: []wire.ManifestEntry{
		fileEntry("done.txt", data, md5sum(t, data), time.Unix(1, 0)),
	}}
	if err := td.dec.decideGroup(context.Background(), gm); err != nil {
		t.Fatalf("decideGroup: %v", err)
	}
	if gd := td.log.only(t); len(gd.Needed) != 0 || gd.SkippedCount != 1 {
		t.Fatalf("decision = %+v, want 0 needed / 1 skipped", gd)
	}
}

// T-PROTO-08: exactly one CREDIT{+1} follows each GROUP_DECISION, in that order.
func TestCreditFollowsEachDecision(t *testing.T) {
	td := newTestDecider(t, Config{}.withDefaults(), fsx.PlatformLinux, nil)
	data := []byte("x")
	gm := &wire.GroupManifest{Entries: []wire.ManifestEntry{
		fileEntry("x", data, md5sum(t, data), time.Unix(1, 0)),
	}}
	if err := td.dec.decideGroup(context.Background(), gm); err != nil {
		t.Fatalf("decideGroup: %v", err)
	}
	var order []string
	for _, m := range td.log.msgs {
		switch m.(type) {
		case *wire.GroupDecision:
			order = append(order, "decision")
		case *wire.Credit:
			order = append(order, "credit")
		}
	}
	if len(order) != 2 || order[0] != "decision" || order[1] != "credit" {
		t.Fatalf("message order = %v, want [decision credit]", order)
	}
	if c, ok := td.log.msgs[len(td.log.msgs)-1].(*wire.Credit); !ok || c.AdditionalGroups != 1 {
		t.Fatalf("last message = %#v, want Credit{1}", td.log.msgs[len(td.log.msgs)-1])
	}
}

// §12.5: a path that escapes the destination root is rejected with its numeric
// code, not fetched.
func TestDecideRejectsUnsafePath(t *testing.T) {
	td := newTestDecider(t, Config{}.withDefaults(), fsx.PlatformLinux, nil)
	data := []byte("evil")
	gm := &wire.GroupManifest{Entries: []wire.ManifestEntry{
		fileEntry("../escape.txt", data, md5sum(t, data), time.Unix(1, 0)),
	}}
	if err := td.dec.decideGroup(context.Background(), gm); err != nil {
		t.Fatalf("decideGroup: %v", err)
	}
	gd := td.log.only(t)
	if len(gd.Rejected) != 1 || gd.Rejected[0].Index != 0 {
		t.Fatalf("Rejected = %+v, want one entry at index 0", gd.Rejected)
	}
	if gd.Rejected[0].ErrorCode == 0 {
		t.Fatalf("rejected entry carries no numeric code")
	}
	if td.cnt.FilesRejected.Load() != 1 {
		t.Fatalf("FilesRejected = %d, want 1", td.cnt.FilesRejected.Load())
	}
}

// §12.6: two manifest paths that collide at a case-insensitive destination —
// the second is rejected E7012.
func TestDecideRejectsCollision(t *testing.T) {
	td := newTestDecider(t, Config{}.withDefaults(), fsx.PlatformDarwin, nil)
	a := []byte("one")
	b := []byte("two")
	gm := &wire.GroupManifest{Entries: []wire.ManifestEntry{
		fileEntry("Report.txt", a, md5sum(t, a), time.Unix(1, 0)),
		fileEntry("report.txt", b, md5sum(t, b), time.Unix(1, 0)),
	}}
	if err := td.dec.decideGroup(context.Background(), gm); err != nil {
		t.Fatalf("decideGroup: %v", err)
	}
	gd := td.log.only(t)
	if len(gd.Rejected) != 1 || gd.Rejected[0].Index != 1 {
		t.Fatalf("Rejected = %+v, want one entry at index 1", gd.Rejected)
	}
	if fault.Code("E"+pad4(gd.Rejected[0].ErrorCode)) != fault.E7012 {
		t.Fatalf("reject code = E%s, want E7012", pad4(gd.Rejected[0].ErrorCode))
	}
}

// §12.7: the free-space guard warns then fails E7003 before any write once the
// running requirement exceeds free space by more than the margin.
func TestDecideFreeSpaceGuardFatal(t *testing.T) {
	td := newTestDecider(t, Config{}.withDefaults(), fsx.PlatformLinux, nil)
	td.dec.guard = newSpaceGuard(1024, 256) // 1 KiB free, 256 B margin
	big := make([]byte, 4096)
	gm := &wire.GroupManifest{Entries: []wire.ManifestEntry{
		fileEntry("big.bin", big, md5sum(t, big), time.Unix(1, 0)),
	}}
	err := td.dec.decideGroup(context.Background(), gm)
	if fault.GetCode(err) != fault.E7003 {
		t.Fatalf("decideGroup err = %v, want E7003", err)
	}
	// The decision is still emitted before the guard trips.
	_ = td.log.only(t)
}

// §12.3: a symlink entry is materialised during decide and a re-run with the
// same target is a no-op.
func TestDecideCreatesSymlink(t *testing.T) {
	td := newTestDecider(t, Config{}.withDefaults(), fsx.PlatformLinux, nil)
	gm := &wire.GroupManifest{Entries: []wire.ManifestEntry{
		{EntryType: entryTypeSymlink, Path: []byte("link"), LinkTarget: []byte("target.txt"), MtimeSec: 1},
	}}
	if err := td.dec.decideGroup(context.Background(), gm); err != nil {
		t.Fatalf("decideGroup: %v", err)
	}
	got, err := os.Readlink(filepath.Join(td.dir, "link"))
	if err != nil || got != "target.txt" {
		t.Fatalf("readlink = %q, err %v", got, err)
	}
	if td.cnt.Symlinks.Load() != 1 {
		t.Fatalf("Symlinks = %d, want 1", td.cnt.Symlinks.Load())
	}
	// Re-run: the decision still reports it as skipped, link unchanged.
	td.log.msgs = nil
	if err := td.dec.decideGroup(context.Background(), gm); err != nil {
		t.Fatalf("decideGroup rerun: %v", err)
	}
	if gd := td.log.only(t); gd.SkippedCount != 1 {
		t.Fatalf("rerun SkippedCount = %d, want 1", gd.SkippedCount)
	}
}

// §12.4: two manifest entries sharing a hardlink key — only the first is
// requested; the second is deferred and linked by fetch.
func TestDecideHardlinkDefersSecondary(t *testing.T) {
	td := newTestDecider(t, Config{}.withDefaults(), fsx.PlatformLinux, nil)
	data := []byte("shared inode contents")
	e1 := fileEntry("first.dat", data, md5sum(t, data), time.Unix(1, 0))
	e1.Flags = wire.ManifestFlagHasHardlinkKey
	e1.HardlinkKey = 777
	e2 := fileEntry("second.dat", data, md5sum(t, data), time.Unix(1, 0))
	e2.Flags = wire.ManifestFlagHasHardlinkKey
	e2.HardlinkKey = 777
	gm := &wire.GroupManifest{Entries: []wire.ManifestEntry{e1, e2}}
	if err := td.dec.decideGroup(context.Background(), gm); err != nil {
		t.Fatalf("decideGroup: %v", err)
	}
	gd := td.log.only(t)
	if len(gd.Needed) != 1 || gd.Needed[0] != 0 {
		t.Fatalf("Needed = %v, want [0] (primary only)", gd.Needed)
	}
	if gd.SkippedCount != 1 {
		t.Fatalf("SkippedCount = %d, want 1 (deferred secondary)", gd.SkippedCount)
	}
	// fetch would call registerMaterialised(0, "first.dat"); simulate it.
	if w := td.dec.hl.registerMaterialised(0, "first.dat"); len(w) != 1 || w[0].rel != "second.dat" || w[0].fileID != 1 {
		t.Fatalf("pending secondaries = %+v, want [{1 second.dat}]", w)
	}
}

// R-16: when the hardlink primary is resolved by SKIP (already present at the
// destination with matching content) rather than a fetch/publish, it never
// reaches fetch.go's post-publish registerMaterialised. Without the decide-time
// fallback, a secondary parked on that key would never be linked and never be
// fetched: present in the manifest, absent from the destination, exit 0.
func TestDecideHardlinkMaterialisesSecondaryOnSkippedPrimary(t *testing.T) {
	td := newTestDecider(t, Config{}.withDefaults(), fsx.PlatformLinux, nil)
	data := []byte("shared inode contents, already on disk")
	// The primary's content is already correct at the destination, so decide
	// will resolve it via SKIP, not a fetch.
	if err := os.WriteFile(filepath.Join(td.dir, "first.dat"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	e1 := fileEntry("first.dat", data, md5sum(t, data), time.Unix(1, 0))
	e1.Flags = wire.ManifestFlagHasHardlinkKey
	e1.HardlinkKey = 777
	e2 := fileEntry("second.dat", data, md5sum(t, data), time.Unix(1, 0))
	e2.Flags = wire.ManifestFlagHasHardlinkKey
	e2.HardlinkKey = 777
	gm := &wire.GroupManifest{Entries: []wire.ManifestEntry{e1, e2}}
	if err := td.dec.decideGroup(context.Background(), gm); err != nil {
		t.Fatalf("decideGroup: %v", err)
	}
	gd := td.log.only(t)
	if len(gd.Needed) != 0 {
		t.Fatalf("Needed = %v, want none (primary skipped, secondary never fetched)", gd.Needed)
	}
	if gd.SkippedCount != 2 {
		t.Fatalf("SkippedCount = %d, want 2", gd.SkippedCount)
	}
	got, err := os.ReadFile(filepath.Join(td.dir, "second.dat"))
	if err != nil {
		t.Fatalf("second.dat must be materialised even though its primary was a SKIP: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("second.dat content = %q, want %q", got, data)
	}
	fi1, err1 := os.Stat(filepath.Join(td.dir, "first.dat"))
	fi2, err2 := os.Stat(filepath.Join(td.dir, "second.dat"))
	if err1 != nil || err2 != nil || !os.SameFile(fi1, fi2) {
		t.Fatalf("second.dat must be hardlinked to first.dat (same inode), got err1=%v err2=%v same=%v",
			err1, err2, err1 == nil && err2 == nil && os.SameFile(fi1, fi2))
	}
}

// directories are created during decide and their metadata is deferred to
// applyDirMeta.
func TestDecideCreatesDirectory(t *testing.T) {
	td := newTestDecider(t, Config{}.withDefaults(), fsx.PlatformLinux, nil)
	mt := time.Unix(1600000000, 0)
	gm := &wire.GroupManifest{Entries: []wire.ManifestEntry{
		{EntryType: entryTypeDir, Path: []byte("sub"), Mode: 0o755, MtimeSec: mt.Unix()},
	}}
	if err := td.dec.decideGroup(context.Background(), gm); err != nil {
		t.Fatalf("decideGroup: %v", err)
	}
	fi, err := os.Stat(filepath.Join(td.dir, "sub"))
	if err != nil || !fi.IsDir() {
		t.Fatalf("stat sub: %v (dir=%v)", err, fi != nil && fi.IsDir())
	}
	td.dec.applyDirMeta(obs.Ctx{})
}

// TestHardlinkClearRetiresTheKey covers the hole the first R-16 fix left. When
// MakeHardlink fails, decide abandons the key and fetches that entry's content
// instead. If clear() drops only keyToPath and leaves the seen bit set, every
// *later* entry sharing the key is parked by claimSecondary on a primary that
// has already published and will never call registerMaterialised again — so it
// is never linked, never fetched and never counted, leaving a hole in the
// destination on an exit-0 run. Three links to one inode plus a single EMLINK
// or EEXIST is enough to hit it.
func TestHardlinkClearRetiresTheKey(t *testing.T) {
	const key = uint64(0xABCD)
	hl := newHardlinkMap()

	// e1 becomes the primary and publishes.
	if hl.claimSecondary(key, 1, "one.dat") {
		t.Fatal("first sighting of a key must not be a secondary")
	}
	hl.markPrimary(1, key)
	if got := hl.registerMaterialised(1, "one.dat"); len(got) != 0 {
		t.Fatalf("no secondaries were parked, got %v", got)
	}

	// e2 finds the primary on disk but its link fails, so it abandons the key.
	if _, ok := hl.pathFor(key); !ok {
		t.Fatal("primary path should be registered")
	}
	if orphans := hl.clear(key); len(orphans) != 0 {
		t.Fatalf("clear returned %v, want no orphans", orphans)
	}

	// e3 must now become a fresh primary and be fetched as content — not park
	// forever waiting on a materialisation that can never come.
	if hl.claimSecondary(key, 3, "three.dat") {
		t.Fatal("after clear the key has no live primary, so the next entry must become one and be fetched, not park as a secondary")
	}
}

// TestHardlinkAbandonSurfacesOrphanedSecondaries covers the other half of the
// R-16 gap: when a hardlink primary fails permanently after every retry, it
// never publishes, so registerMaterialised is never reached and the
// secondaries parked on its key are left unlinked, unfetched and — the part
// that matters — uncounted. The run then reports exactly one failure while
// several files are missing from the destination.
func TestHardlinkAbandonSurfacesOrphanedSecondaries(t *testing.T) {
	const key = uint64(0x5151)
	hl := newHardlinkMap()

	if hl.claimSecondary(key, 1, "primary.dat") {
		t.Fatal("first sighting of a key must not be a secondary")
	}
	hl.markPrimary(1, key)
	if !hl.claimSecondary(key, 2, "second.dat") || !hl.claimSecondary(key, 3, "third.dat") {
		t.Fatal("entries sharing a pending primary's key must park as secondaries")
	}

	orphans := hl.abandon(1)
	if len(orphans) != 2 {
		t.Fatalf("abandon returned %d secondaries, want 2: the caller cannot count what it is not handed", len(orphans))
	}
	got := map[string]bool{orphans[0].rel: true, orphans[1].rel: true}
	if !got["second.dat"] || !got["third.dat"] {
		t.Errorf("abandon returned %v, want second.dat and third.dat", orphans)
	}
	if again := hl.abandon(1); len(again) != 0 {
		t.Errorf("abandon must retire the key; second call returned %v", again)
	}
}
