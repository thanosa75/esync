package plan

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"esync/internal/crypto/record"
	"esync/internal/obs"
	"esync/internal/wire"
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

// T-SCAN-02: grouping math. file_id = sorted index; groups are packed to a byte
// target but never exceed maxGroupEntries entries; the last group holds the
// remainder. Empty -> 0 groups, 1 -> 1 group of 1.
func TestGroupingMath(t *testing.T) {
	if g := mustBuild(t, nil).NumGroups(); g != 0 {
		t.Fatalf("empty input: NumGroups = %d, want 0", g)
	}
	if g := mustBuild(t, []Entry{fileEntry("x", 1, 1)}).NumGroups(); g != 1 {
		t.Fatalf("one entry: NumGroups = %d, want 1", g)
	}

	// Many tiny files well under the byte target: the 1024-entry cap splits them.
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
	if !ok || id != 0 || p.GroupID(id) != 0 {
		t.Fatalf("FileID(f/000000) = %d,%v groupID %d", id, ok, p.GroupID(id))
	}
	id, _ = p.FileID("f/002049")
	if id != 2049 || p.GroupID(id) != 2 || p.GroupFirstID(2) != 2048 {
		t.Fatalf("FileID(f/002049) = %d groupID %d first %d, want 2049/2/2048", id, p.GroupID(id), p.GroupFirstID(2))
	}
}

// T-SCAN-02: the byte target closes a group before the entry cap when files are
// large, and an oversize single file forms its own group. Files sort as
// a,b,big,c,d by name.
func TestGroupingBySize(t *testing.T) {
	const mib = 1 << 20
	es := []Entry{
		fileEntry("a", 200*mib, 1),   // group 0
		fileEntry("b", 200*mib, 1),   // group 0 (a+b = 400 MiB <= 512)
		fileEntry("big", 900*mib, 1), // group 1 (400+900 > 512): oversize, alone
		fileEntry("c", 300*mib, 1),   // group 2 (900+300 > 512)
		fileEntry("d", 300*mib, 1),   // group 3 (300+300 > 512)
	}
	p, err := Build(es, Options{GroupBytes: 512 * mib})
	if err != nil {
		t.Fatal(err)
	}
	if p.NumGroups() != 4 {
		t.Fatalf("NumGroups = %d, want 4", p.NumGroups())
	}
	got := []int{len(p.Group(0)), len(p.Group(1)), len(p.Group(2)), len(p.Group(3))}
	if got[0] != 2 || got[1] != 1 || got[2] != 1 || got[3] != 1 {
		t.Fatalf("group sizes = %v, want [2 1 1 1]", got)
	}
	firsts := p.GroupFirsts()
	if len(firsts) != 4 || firsts[0] != 0 || firsts[1] != 2 || firsts[2] != 3 || firsts[3] != 4 {
		t.Fatalf("GroupFirsts = %v, want [0 2 3 4]", firsts)
	}
	if p.GroupID(2) != 1 || p.GroupID(4) != 3 {
		t.Fatalf("GroupID(2)=%d GroupID(4)=%d, want 1/3", p.GroupID(2), p.GroupID(4))
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

// R-13 / T-SCAN-02: a 1024-entry group with long paths would encode a
// GROUP_MANIFEST bigger than record.MaxCTControl if grouping only capped entry
// count and file bytes. The manifest-size cap must split it into more groups,
// each of which stays within wire.MaxGroupManifestEntryBytes.
func TestGroupingByManifestSize(t *testing.T) {
	const n = 1024
	longName := strings.Repeat("x", 450)
	var es []Entry
	for i := 0; i < n; i++ {
		es = append(es, fileEntry(fmt.Sprintf("d/%s/%04d", longName, i), 10, 1))
	}
	p := mustBuild(t, es)
	if p.NumGroups() <= 1 {
		t.Fatalf("NumGroups = %d, want > 1 (manifest-size cap should have split this group)", p.NumGroups())
	}

	total := 0
	for gi := 0; gi < p.NumGroups(); gi++ {
		group := p.Group(gi)
		if len(group) == 0 {
			t.Fatalf("group %d is empty", gi)
		}
		total += len(group)
		var sum int64
		for i := range group {
			sum += int64(manifestEntrySize(&group[i]))
		}
		if sum > maxGroupManifestEntryBytes {
			t.Fatalf("group %d encoded manifest bytes = %d, want <= %d", gi, sum, maxGroupManifestEntryBytes)
		}
	}
	if total != n {
		t.Fatalf("total entries across groups = %d, want %d", total, n)
	}
}

// R-13: a single entry whose own encoded size already exceeds
// maxGroupManifestEntryBytes must still get its own one-entry group — not an
// infinite loop, not a zero-entry group, and it must not swallow the next
// entry into the same (already oversize) group.
func TestGroupingByManifestSizeSingleOversizeEntry(t *testing.T) {
	huge := strings.Repeat("x", 600_000) // encoded size alone exceeds the budget
	es := []Entry{
		fileEntry(huge, 10, 1),
		fileEntry("small", 5, 1),
	}
	p := mustBuild(t, es)
	if p.NumGroups() != 2 {
		t.Fatalf("NumGroups = %d, want 2 (oversize entry isolated into its own group)", p.NumGroups())
	}
	if len(p.Group(0)) != 1 || len(p.Group(1)) != 1 {
		t.Fatalf("group sizes = %d/%d, want 1/1", len(p.Group(0)), len(p.Group(1)))
	}
}

// R-13: the largest group the real packing algorithm forms out of
// maximum-length (wire.maxPathBytes) paths, encoded exactly as
// sender/manifest.go would (worst-case max-length digests filled in), must
// still fit inside one control-channel record.
func TestRealisticWorstCaseGroupFitsControlRecord(t *testing.T) {
	pathPrefix := strings.Repeat("p", 4090) // + "/dddd" = 4095 bytes, under the wire's 4096 cap
	var es []Entry
	for i := 0; i < maxGroupEntries; i++ {
		es = append(es, fileEntry(fmt.Sprintf("%s/%04d", pathPrefix, i), 10, 1))
	}
	p := mustBuild(t, es)

	group := p.Group(0)
	if len(group) == 0 || len(group) >= maxGroupEntries {
		t.Fatalf("group 0 size = %d, want a manifest-size-capped group well under %d", len(group), maxGroupEntries)
	}
	gm := &wire.GroupManifest{GroupID: 0, FirstFileID: 0, Entries: make([]wire.ManifestEntry, len(group))}
	for i, e := range group {
		gm.Entries[i] = wire.ManifestEntry{
			EntryType: uint8(e.Type),
			Path:      e.RelPath,
			Size:      uint64(e.Size),
			MtimeSec:  e.MtimeSec,
			Digest:    bytes.Repeat([]byte{0xAB}, wire.MaxDigestBytes),
		}
	}
	body, err := wire.Marshal(gm)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	kEnc := obs.NewSecret(bytes.Repeat([]byte{0x01}, 32))
	kMac := obs.NewSecret(bytes.Repeat([]byte{0x02}, 32))
	var buf bytes.Buffer
	w := record.NewWriter(&buf, kEnc, kMac, 0)
	if err := w.WriteFrame(body); err != nil {
		t.Fatalf("worst-case group manifest exceeds the control-record ceiling: %v", err)
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
