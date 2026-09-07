package plan

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

func fileEntry(p string, size int64, mtime int64) Entry {
	return Entry{RelPath: []byte(p), Type: TypeFile, Size: size, MtimeSec: mtime}
}

// e-acute and i-diaeresis, precomposed (NFC) vs decomposed (NFD).
const (
	eAcuteNFC = "é"
	eAcuteNFD = "é"
	iDiaerNFC = "ï"
	iDiaerNFD = "ï"
)

// P-SCAN-01: a tree fed twice (here: shuffled) yields identical file ids, groups
// and manifest digest. [T-SCAN-03, REQ-SCAN-020]
func TestDeterminism(t *testing.T) {
	names := []string{
		"a.txt", "z/b.txt", "z/a.txt", "m/n/o.txt", "B.txt", "caf" + eAcuteNFC + ".txt",
		"dir/", "z/", "m/", "m/n/", "0", "~tmp", "Ünicode/x",
	}
	var base []Entry
	for i, n := range names {
		typ := TypeFile
		if bytes.HasSuffix([]byte(n), []byte("/")) {
			typ = TypeDir
			n = n[:len(n)-1]
		}
		base = append(base, Entry{RelPath: []byte(n), Type: typ, Size: int64(i * 7), MtimeSec: int64(1000 + i)})
	}

	p1, err := Build(append([]Entry(nil), base...), Options{})
	if err != nil {
		t.Fatal(err)
	}

	shuf := append([]Entry(nil), base...)
	rand.New(rand.NewSource(42)).Shuffle(len(shuf), func(i, j int) { shuf[i], shuf[j] = shuf[j], shuf[i] })
	p2, err := Build(shuf, Options{})
	if err != nil {
		t.Fatal(err)
	}

	if p1.ManifestDigest() != p2.ManifestDigest() {
		t.Fatalf("digest not stable under input order: %x vs %x", p1.ManifestDigest(), p2.ManifestDigest())
	}
	for i := range p1.Entries() {
		if !bytes.Equal(p1.Entries()[i].RelPath, p2.Entries()[i].RelPath) {
			t.Fatalf("order differs at %d: %q vs %q", i, p1.Entries()[i].RelPath, p2.Entries()[i].RelPath)
		}
	}
}

// T-SCAN-03 / REQ-SCAN-020: NFD and NFC spellings of the same names yield the
// SAME manifest digest.
func TestNFDvsNFCSameDigest(t *testing.T) {
	nfc := "caf" + eAcuteNFC + "/na" + iDiaerNFC + "ve.txt"
	nfd := "caf" + eAcuteNFD + "/na" + iDiaerNFD + "ve.txt"
	mixed := "caf" + eAcuteNFD + "/na" + iDiaerNFC + "ve.txt"

	if nfc == nfd {
		t.Fatal("test bug: NFC and NFD strings are byte-identical")
	}
	d0 := mustBuild(t, []Entry{fileEntry(nfc, 10, 5)}).ManifestDigest()
	d1 := mustBuild(t, []Entry{fileEntry(nfd, 10, 5)}).ManifestDigest()
	d2 := mustBuild(t, []Entry{fileEntry(mixed, 10, 5)}).ManifestDigest()
	if d0 != d1 || d0 != d2 {
		t.Fatalf("NFD/NFC digests differ: %x %x %x", d0, d1, d2)
	}
}

// P-SCAN-02: the ordering is a strict total order over Unicode edge cases and
// invalid UTF-8.
func TestOrderingIsTotalOrder(t *testing.T) {
	names := [][]byte{
		[]byte(""), []byte("a"), []byte("A"), []byte("a/b"), []byte("a.b"),
		[]byte("ab"), []byte("caf" + eAcuteNFC), []byte("caf" + eAcuteNFD), []byte(eAcuteNFC),
		[]byte(eAcuteNFD), []byte("z"), []byte("z/a"), []byte("z0"),
		{0xff, 0xfe}, {0xff}, {0x80, 0x80}, []byte("\xc3\x28"), []byte("\xed\xa0\x80"),
	}
	cmp := func(a, b []byte) int {
		ak, _ := orderingKey(toSlash(a))
		bk, _ := orderingKey(toSlash(b))
		if c := bytes.Compare(ak, bk); c != 0 {
			return c
		}
		return bytes.Compare(toSlash(a), toSlash(b))
	}
	for _, a := range names {
		if cmp(a, a) != 0 {
			t.Errorf("not reflexive-zero: %q", a)
		}
		for _, b := range names {
			if sign(cmp(a, b)) != -sign(cmp(b, a)) {
				t.Errorf("not antisymmetric: %q %q", a, b)
			}
			for _, c := range names {
				if cmp(a, b) < 0 && cmp(b, c) < 0 && !(cmp(a, c) < 0) {
					t.Errorf("not transitive: %q %q %q", a, b, c)
				}
			}
		}
	}
}

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}

// T-SCAN-02: grouping math. file_id = sorted index, group_id = file_id/1024,
// last group holds the remainder; empty -> 0 groups, 1 -> 1 group of 1.
func TestGroupingMath(t *testing.T) {
	if g := mustBuild(t, nil).NumGroups(); g != 0 {
		t.Fatalf("empty input: NumGroups = %d, want 0", g)
	}
	if g := mustBuild(t, []Entry{fileEntry("x", 1, 1)}).NumGroups(); g != 1 {
		t.Fatalf("one entry: NumGroups = %d, want 1", g)
	}

	var es []Entry
	const n = 2050
	for i := 0; i < n; i++ {
		es = append(es, fileEntry(fmt.Sprintf("f/%06d", i), int64(i), 1))
	}
	p := mustBuild(t, es)
	if p.NumGroups() != 3 {
		t.Fatalf("NumGroups = %d, want 3", p.NumGroups())
	}
	if len(p.Group(0)) != 1024 || len(p.Group(1)) != 1024 || len(p.Group(2)) != 2 {
		t.Fatalf("group sizes = %d/%d/%d, want 1024/1024/2", len(p.Group(0)), len(p.Group(1)), len(p.Group(2)))
	}
	id, ok := p.FileID("f/000000")
	if !ok || id != 0 || GroupID(id) != 0 {
		t.Fatalf("FileID(f/000000) = %d,%v groupID %d", id, ok, GroupID(id))
	}
	id, _ = p.FileID("f/002049")
	if id != 2049 || GroupID(id) != 2 {
		t.Fatalf("FileID(f/002049) = %d groupID %d, want 2049/2", id, GroupID(id))
	}
}

// T-SCAN-02: totals count regular files only; dirs/symlinks still take id slots.
func TestTotals(t *testing.T) {
	es := []Entry{
		fileEntry("a", 100, 1),
		fileEntry("b", 200, 1),
		{RelPath: []byte("d"), Type: TypeDir, MtimeSec: 1},
		{RelPath: []byte("l"), Type: TypeSymlink, LinkTarget: []byte("a"), MtimeSec: 1},
	}
	p := mustBuild(t, es)
	if p.TotalFiles != 2 || p.TotalBytes != 300 {
		t.Fatalf("TotalFiles=%d TotalBytes=%d, want 2/300", p.TotalFiles, p.TotalBytes)
	}
	if len(p.Entries()) != 4 {
		t.Fatalf("entries = %d, want 4", len(p.Entries()))
	}
}

func mustBuild(t *testing.T, es []Entry) *Plan {
	t.Helper()
	p, err := Build(es, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
