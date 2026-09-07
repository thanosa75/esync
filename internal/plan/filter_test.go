package plan

import (
	"testing"

	"esync/internal/fault"
)

// T-SCAN-06: the filter signature is stable for a given rule set and changes
// when the rules change.
func TestFilterSignatureStable(t *testing.T) {
	o := Options{Filters: []FilterRule{
		{Include: false, Pattern: "*.tmp"},
		{Include: true, Pattern: "keep/*.tmp"},
	}}
	if o.FilterSignature() != o.FilterSignature() {
		t.Fatal("signature not deterministic")
	}
	o2 := Options{Filters: []FilterRule{{Include: false, Pattern: "*.tmp"}}}
	if o.FilterSignature() == o2.FilterSignature() {
		t.Fatal("different rule sets produced the same signature")
	}
	// order matters (last match wins)
	o3 := Options{Filters: []FilterRule{
		{Include: true, Pattern: "keep/*.tmp"},
		{Include: false, Pattern: "*.tmp"},
	}}
	if o.FilterSignature() == o3.FilterSignature() {
		t.Fatal("rule order not reflected in the signature")
	}
}

// REQ-CLI-011: last match wins; a --include cannot re-admit a sys-excluded entry.
func TestFilters(t *testing.T) {
	es := []Entry{
		fileEntry("src/a.go", 1, 1),
		fileEntry("src/a.tmp", 1, 1),
		fileEntry("src/keep/b.tmp", 1, 1),
		fileEntry("Thumbs.db", 1, 1),
		{RelPath: []byte("build"), Type: TypeDir, MtimeSec: 1},
	}
	opts := Options{Filters: []FilterRule{
		{Include: false, Pattern: "*.tmp"},
		{Include: true, Pattern: "src/keep/*.tmp"},
		{Include: false, Pattern: "build/"},
	}}
	p := mustBuild2(t, es, opts)
	got := names(p)
	if !got["src/a.go"] {
		t.Error("src/a.go should be kept")
	}
	if got["src/a.tmp"] {
		t.Error("src/a.tmp should be excluded by *.tmp")
	}
	if !got["src/keep/b.tmp"] {
		t.Error("src/keep/b.tmp should be re-included by src/keep/*.tmp")
	}
	if got["build"] {
		t.Error("build/ dir should be excluded")
	}
	if got["Thumbs.db"] {
		t.Error("a filter must not re-admit; Thumbs.db still excluded by sys set")
	}
}

func TestBadFilterPattern(t *testing.T) {
	_, err := Build(nil, Options{Filters: []FilterRule{{Pattern: "a[b"}}})
	if fault.GetCode(err) != fault.E1007 {
		t.Fatalf("bad pattern: got %v, want E1007", err)
	}
}
