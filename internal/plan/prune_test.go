package plan

import (
	"math/rand"
	"testing"
)

// Item-3 / REQ-SCAN-026: excluded directories are PRUNED, not descended into
// (ARCHITECTURE §10.1). Their contents must never reach the plan: no file ids,
// no digest contribution, and no per-entry exclusion records or counts — only
// the pruned directory itself is recorded.
func TestPruneSysDirContents(t *testing.T) {
	es := []Entry{
		{RelPath: []byte("__MACOSX"), Type: TypeDir, MtimeSec: 1},
		fileEntry("__MACOSX/x", 5, 1),
		fileEntry("__MACOSX/._kept", 6, 1),
		fileEntry("__MACOSX/deep/y.txt", 7, 1),
		fileEntry("k.txt", 1, 1),
		fileEntry("doc/plain.txt", 1, 1), // sys file drops live under kept dirs
		{RelPath: []byte(".Trash-1000"), Type: TypeDir, MtimeSec: 1},
		fileEntry(".Trash-1000/a", 9, 1),
		{RelPath: []byte("vol/.Trashes"), Type: TypeDir, MtimeSec: 1},
		fileEntry("vol/.Trashes/x", 9, 1), // pruned dir at a deeper level
	}
	p := mustBuild(t, es)
	got := names(p)
	if !got["k.txt"] || !got["doc/plain.txt"] {
		t.Fatal("kept files missing")
	}
	for _, dropped := range []string{
		"__MACOSX", "__MACOSX/x", "__MACOSX/._kept", "__MACOSX/deep/y.txt",
		".Trash-1000", ".Trash-1000/a", "vol/.Trashes", "vol/.Trashes/x",
	} {
		if got[dropped] {
			t.Errorf("%q survived pruning", dropped)
		}
	}
	// Counted and recorded once each (the pruned dirs only), never per child.
	if p.ExcludedCount != 3 {
		t.Errorf("ExcludedCount = %d, want 3 (pruned dirs only)", p.ExcludedCount)
	}
	if len(p.Exclusions()) != 3 {
		t.Fatalf("exclusion records = %d, want 3", len(p.Exclusions()))
	}
	for _, x := range p.Exclusions() {
		if !x.Prune {
			t.Errorf("recorded exclusion %q should be a prune", x.Path)
		}
	}
}

// The same property at the digest level: a tree whose excluded-dir contents were
// walked must plan identically to one where they were never present.
func TestPruneDigestEqualsUnwalked(t *testing.T) {
	withContents := []Entry{
		{RelPath: []byte("__MACOSX"), Type: TypeDir, MtimeSec: 1},
		fileEntry("__MACOSX/x", 5, 2),
		fileEntry("__MACOSX/.DS_Store", 6, 2),
		fileEntry("__MACOSX/a/b.txt", 7, 2),
		{RelPath: []byte(".Trash-1000"), Type: TypeDir, MtimeSec: 1},
		fileEntry(".Trash-1000/deep", 8, 2),
		fileEntry("k.txt", 1, 2),
	}
	neverWalked := []Entry{
		{RelPath: []byte("__MACOSX"), Type: TypeDir, MtimeSec: 1},
		{RelPath: []byte(".Trash-1000"), Type: TypeDir, MtimeSec: 1},
		fileEntry("k.txt", 1, 2),
	}
	a := mustBuild(t, withContents)
	b := mustBuild(t, neverWalked)
	if a.ManifestDigest() != b.ManifestDigest() {
		t.Fatalf("pruned-tree digest %x != never-walked digest %x", a.ManifestDigest(), b.ManifestDigest())
	}
	if a.TotalFiles != b.TotalFiles || len(a.Entries()) != len(b.Entries()) {
		t.Fatalf("totals differ: %d/%d vs %d/%d", a.TotalFiles, len(a.Entries()), b.TotalFiles, len(b.Entries()))
	}
	if a.ExcludedCount != b.ExcludedCount {
		t.Fatalf("ExcludedCount differs: %d vs %d", a.ExcludedCount, b.ExcludedCount)
	}
}

// REQ-CLI-011 / §10.1: a user filter that excludes a directory prunes its whole
// subtree, and a later rule cannot re-admit a path beneath it — matching the
// walker-prunes semantics the filter language always claimed.
func TestPruneFilterDirContents(t *testing.T) {
	es := []Entry{
		{RelPath: []byte("build"), Type: TypeDir, MtimeSec: 1},
		fileEntry("build/a.o", 5, 1),
		fileEntry("build/sub/b.o", 6, 1),
		{RelPath: []byte("node_modules"), Type: TypeDir, MtimeSec: 1},
		fileEntry("node_modules/pkg/index.js", 7, 1),
		fileEntry("src/main.go", 1, 1),
		fileEntry("keep.me", 1, 1),
	}
	opts := Options{Filters: []FilterRule{
		{Include: false, Pattern: "build/"},
		{Include: false, Pattern: "node_modules"},
		{Include: true, Pattern: "build/keep/a.c"}, // must NOT re-admit under pruned build/
	}}
	p := mustBuild2(t, es, opts)
	got := names(p)
	for _, want := range []string{"src/main.go", "keep.me"} {
		if !got[want] {
			t.Errorf("%q should be kept", want)
		}
	}
	for _, dropped := range []string{
		"build", "build/a.o", "build/sub", "build/sub/b.o",
		"node_modules", "node_modules/pkg", "node_modules/pkg/index.js",
	} {
		if got[dropped] {
			t.Errorf("%q survived filter pruning", dropped)
		}
	}
	if p.ExcludedCount != 2 {
		t.Errorf("ExcludedCount = %d, want 2 (build + node_modules)", p.ExcludedCount)
	}
}

// P-SCAN-01 extension: pruning is a pure function of the entry set — shuffling
// the input (a child appearing before its pruned parent) changes nothing.
func TestPruneOrderIndependent(t *testing.T) {
	es := []Entry{
		{RelPath: []byte("out"), Type: TypeDir, MtimeSec: 1},
		fileEntry("out/1.bin", 5, 1),
		fileEntry("out/sub/2.bin", 6, 1),
		fileEntry("keep.txt", 1, 1),
		{RelPath: []byte("__MACOSX"), Type: TypeDir, MtimeSec: 1},
		fileEntry("__MACOSX/._x", 7, 1),
	}
	opts := Options{Filters: []FilterRule{{Include: false, Pattern: "out/"}}}

	dfs := mustBuild2(t, es, opts)

	shuf := append([]Entry(nil), es...)
	rand.New(rand.NewSource(7)).Shuffle(len(shuf), func(i, j int) { shuf[i], shuf[j] = shuf[j], shuf[i] })
	other := mustBuild2(t, shuf, opts)

	if dfs.ManifestDigest() != other.ManifestDigest() {
		t.Fatal("digest differs under input order")
	}
	if dfs.ExcludedCount != other.ExcludedCount || len(dfs.Entries()) != len(other.Entries()) {
		t.Fatal("exclusions/totals differ under input order")
	}
	for i := range dfs.Entries() {
		if string(dfs.Entries()[i].RelPath) != string(other.Entries()[i].RelPath) {
			t.Fatalf("entries differ at %d", i)
		}
	}
}
