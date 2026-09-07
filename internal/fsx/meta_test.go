package fsx

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"esync/internal/fault"
)

// §12.3 / §12.5: EnsureDir creates the tree and applies the leaf mode; ApplyMeta
// reconciles mode + mtime at group completion; Discard wipes the journal.
func TestEnsureDirAndApplyMeta(t *testing.T) {
	d, dir := newDest(t)

	if err := d.EnsureDir("a/b/c", 0o750); err != nil {
		t.Fatalf("EnsureDir: %v", err)
	}
	fi, err := os.Stat(filepath.Join(dir, "a/b/c"))
	if err != nil || !fi.IsDir() {
		t.Fatalf("dir not created: %v", err)
	}
	if fi.Mode().Perm() != 0o750 {
		t.Errorf("leaf mode = %v, want 0750", fi.Mode().Perm())
	}
	if err := d.EnsureDir("", 0o755); err != nil {
		t.Errorf("EnsureDir(\"\") should be a no-op: %v", err)
	}

	mtime := time.Unix(1_600_000_000, 0)
	if err := d.ApplyMeta("a/b/c", 0o700, mtime); err != nil {
		t.Fatalf("ApplyMeta: %v", err)
	}
	fi, _ = os.Stat(filepath.Join(dir, "a/b/c"))
	if fi.Mode().Perm() != 0o700 || !fi.ModTime().Equal(mtime) {
		t.Errorf("ApplyMeta not applied: mode=%v mtime=%v", fi.Mode().Perm(), fi.ModTime())
	}
	if err := d.ApplyMeta("nope", 0o700, mtime); fault.GetCode(err) != fault.E7006 {
		t.Fatalf("ApplyMeta on missing path: got %v, want E7006", err)
	}

	if got := d.Dir(); got != dir {
		t.Errorf("Dir() = %q, want %q", got, dir)
	}
	if d.Root() == nil {
		t.Error("Root() is nil")
	}
}

func TestChown(t *testing.T) {
	d, dir := newDest(t)
	if err := os.WriteFile(filepath.Join(dir, "f"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	// chown to our own ids succeeds; to a foreign id it warns with E7006.
	if err := d.Chown("f", os.Getuid(), os.Getgid()); err != nil {
		t.Fatalf("chown to self: %v", err)
	}
	if os.Getuid() != 0 {
		if err := d.Chown("f", 0, 0); fault.GetCode(err) != fault.E7006 {
			t.Fatalf("chown to root: got %v, want E7006", err)
		}
	}
}

func TestJournalDiscard(t *testing.T) {
	dir := t.TempDir()
	j, _, err := Open(dir, "s", digestA)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.MarkComplete(Record{FileID: 0, Path: "x", Digest: []byte{1}, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if err := j.Discard(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".esync")); !os.IsNotExist(err) {
		t.Fatal(".esync/ should be gone after Discard")
	}
}
