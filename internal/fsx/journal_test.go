package fsx

import (
	"os"
	"path/filepath"
	"testing"

	"esync/internal/fault"
)

var digestA = [32]byte{1, 2, 3}
var digestB = [32]byte{9, 9, 9}

// T-RES-01: journal round-trip — a resumed session sees the completed files.
func TestJournalRoundTrip(t *testing.T) {
	dir := t.TempDir()

	j, prior, err := Open(dir, "sess-1", digestA)
	if err != nil {
		t.Fatal(err)
	}
	if j.Resumed || len(prior) != 0 {
		t.Fatalf("fresh session: Resumed=%v prior=%d", j.Resumed, len(prior))
	}
	recs := []Record{
		{FileID: 0, Path: "a/b.txt", Digest: []byte{0xaa}, Size: 10},
		{FileID: 5, Path: "c d/e.txt", Digest: []byte{0xbb, 0xcc}, Size: 20},
	}
	for _, r := range recs {
		if err := j.MarkComplete(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}
	j.Close()

	j2, prior2, err := Open(dir, "sess-2", digestA)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	if !j2.Resumed {
		t.Fatal("second Open with the same digest should resume")
	}
	if len(prior2) != 2 {
		t.Fatalf("prior records = %d, want 2", len(prior2))
	}
	if got := prior2[5]; got.Path != "c d/e.txt" || got.Size != 20 || string(got.Digest) != "\xbb\xcc" {
		t.Fatalf("record 5 round-tripped wrong: %+v", got)
	}
}

// T-RES-02: a torn final record is discarded on read.
func TestJournalTornTail(t *testing.T) {
	dir := t.TempDir()
	j, _, err := Open(dir, "s", digestA)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.MarkComplete(Record{FileID: 1, Path: "ok.txt", Digest: []byte{1}, Size: 1}); err != nil {
		t.Fatal(err)
	}
	j.Sync()
	j.Close()

	// append a torn/garbage record
	logPath := filepath.Join(dir, ".esync", "complete.log")
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("2 999 deadbeef bm9wZQ== ffffffff\n") // wrong CRC
	f.Close()

	_, prior, err := Open(dir, "s2", digestA)
	if err != nil {
		t.Fatal(err)
	}
	if len(prior) != 1 {
		t.Fatalf("prior = %d, want 1 (torn record discarded)", len(prior))
	}
	if _, ok := prior[1]; !ok {
		t.Fatal("the valid record before the torn one should survive")
	}
}

// T-RES-03: Clear removes the whole .esync/ residue; a digest mismatch discards.
func TestJournalClearAndMismatch(t *testing.T) {
	dir := t.TempDir()

	j, _, err := Open(dir, "s", digestA)
	if err != nil {
		t.Fatal(err)
	}
	j.MarkComplete(Record{FileID: 0, Path: "x", Digest: []byte{1}, Size: 1})
	j.Sync()

	// re-open with a different digest → prior discarded, fresh session
	j.Close()
	j2, prior, err := Open(dir, "s", digestB)
	if err != nil {
		t.Fatal(err)
	}
	if j2.Resumed || len(prior) != 0 {
		t.Fatalf("digest mismatch should discard: Resumed=%v prior=%d", j2.Resumed, len(prior))
	}

	if err := j2.Clear(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".esync")); !os.IsNotExist(err) {
		t.Fatal(".esync/ should be gone after Clear")
	}
}

func TestJournalWriteAfterClose(t *testing.T) {
	dir := t.TempDir()
	j, _, err := Open(dir, "s", digestA)
	if err != nil {
		t.Fatal(err)
	}
	j.Close()
	if err := j.MarkComplete(Record{FileID: 1}); fault.GetCode(err) != fault.E7007 {
		t.Fatalf("write after close: got %v, want E7007", err)
	}
}
