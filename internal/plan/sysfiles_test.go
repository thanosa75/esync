package plan

import (
	"bytes"
	"testing"
)

func names(p *Plan) map[string]bool {
	m := make(map[string]bool, len(p.Entries()))
	for _, e := range p.Entries() {
		m[string(e.RelPath)] = true
	}
	return m
}

func excluded(p *Plan, path string) (Exclusion, bool) {
	for _, x := range p.Exclusions() {
		if string(x.Path) == path {
			return x, true
		}
	}
	return Exclusion{}, false
}

// T-SCAN-13: all three Thumbs.db casings are dropped (Windows ASCII case fold).
// T-SCAN-14: lost+found / RECYCLED dropped at root, KEPT when nested.
// T-SCAN-17: ._x prefix drop; pruned dirs flagged.
func TestSystemFileExclusion(t *testing.T) {
	es := []Entry{
		fileEntry("Thumbs.db", 1, 1),
		fileEntry("a/thumbs.db", 1, 1),
		fileEntry("a/b/THUMBS.DB", 1, 1),
		fileEntry("._resource", 1, 1),
		fileEntry("doc/._notes", 1, 1),
		{RelPath: []byte("lost+found"), Type: TypeDir, MtimeSec: 1},
		{RelPath: []byte("deep/lost+found"), Type: TypeDir, MtimeSec: 1},
		{RelPath: []byte("RECYCLED"), Type: TypeDir, MtimeSec: 1},
		{RelPath: []byte("x/RECYCLED"), Type: TypeDir, MtimeSec: 1},
		{RelPath: []byte("__MACOSX"), Type: TypeDir, MtimeSec: 1},
		fileEntry("keep.txt", 1, 1),
	}
	p := mustBuild(t, es)
	got := names(p)

	for _, dropped := range []string{
		"Thumbs.db", "a/thumbs.db", "a/b/THUMBS.DB", "._resource", "doc/._notes",
		"lost+found", "RECYCLED", "__MACOSX",
	} {
		if got[dropped] {
			t.Errorf("%q should have been excluded", dropped)
		}
	}
	for _, kept := range []string{"deep/lost+found", "x/RECYCLED", "keep.txt"} {
		if !got[kept] {
			t.Errorf("%q should have been kept (not root-level)", kept)
		}
	}
	if p.ExcludedCount != 8 {
		t.Errorf("ExcludedCount = %d, want 8", p.ExcludedCount)
	}
	if x, ok := excluded(p, "__MACOSX"); !ok || !x.Prune {
		t.Errorf("__MACOSX should be reported as a pruned directory: %+v", x)
	}
	if x, ok := excluded(p, "Thumbs.db"); !ok || x.Prune {
		t.Errorf("Thumbs.db should be a plain drop, not prune: %+v", x)
	}
	if x, _ := excluded(p, "lost+found"); x.Rule != "linux:lost+found" {
		t.Errorf("lost+found matched rule = %q", x.Rule)
	}
}

// T-SCAN-17: --keep-system-files disables the whole set.
func TestKeepSystemFiles(t *testing.T) {
	es := []Entry{
		fileEntry("Thumbs.db", 1, 1),
		fileEntry(".DS_Store", 1, 1),
		{RelPath: []byte("__MACOSX"), Type: TypeDir, MtimeSec: 1},
	}
	p := mustBuild2(t, es, Options{KeepSystemFiles: true})
	if len(p.Entries()) != 3 || p.ExcludedCount != 0 {
		t.Fatalf("entries=%d excluded=%d, want 3/0", len(p.Entries()), p.ExcludedCount)
	}
}

// T-SCAN-15 / T-SCAN-16: the sys tag participates in the manifest digest, so
// toggling --keep-system-files changes the plan identity.
func TestSysTagInDigest(t *testing.T) {
	es := []Entry{fileEntry("keep.txt", 1, 1)}
	on := mustBuild2(t, es, Options{})
	off := mustBuild2(t, es, Options{KeepSystemFiles: true})
	if on.ManifestDigest() == off.ManifestDigest() {
		t.Fatal("keep-system-files did not change the manifest digest")
	}
	if !bytes.Contains([]byte(off.FilterSignature()), []byte("sysexclude=off")) {
		t.Errorf("filter signature does not record the toggle: %q", off.FilterSignature())
	}
}

// Direct exercise of MatchSystemFile's root-only and prefix semantics.
func TestMatchSystemFile(t *testing.T) {
	cases := []struct {
		name          string
		isDir, atRoot bool
		wantMatch     bool
		wantPrune     bool
	}{
		{".DS_Store", false, false, true, false},
		{"._x", false, false, true, false},
		{".Trash-1000", true, false, true, true},
		{"lost+found", true, false, false, false}, // not at root
		{"lost+found", true, true, true, true},
		{"lost+found", false, true, true, false}, // file, so dropped not pruned
		{"regular.txt", false, true, false, false},
		{"$recycle.bin", true, true, true, true}, // windows fold
	}
	for _, c := range cases {
		rule, prune, ok := MatchSystemFile([]byte(c.name), c.isDir, c.atRoot)
		if ok != c.wantMatch || prune != c.wantPrune {
			t.Errorf("%q (dir=%v root=%v): match=%v prune=%v rule=%q, want match=%v prune=%v",
				c.name, c.isDir, c.atRoot, ok, prune, rule, c.wantMatch, c.wantPrune)
		}
	}
}

func mustBuild2(t *testing.T, es []Entry, o Options) *Plan {
	t.Helper()
	p, err := Build(es, o)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
