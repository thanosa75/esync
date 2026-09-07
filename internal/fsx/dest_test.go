package fsx

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"esync/internal/fault"
)

func newDest(t *testing.T) (*Dest, string) {
	t.Helper()
	dir := t.TempDir()
	d, err := OpenDest(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d, dir
}

// T-XFER-03: atomic publish — the final path only ever sees a complete file.
func TestAtomicPublish(t *testing.T) {
	d, dir := newDest(t)

	f, err := d.OpenPart(7)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("hello world"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	final := filepath.Join(dir, "sub", "out.txt")
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatal("final path exists before publish")
	}

	mtime := time.Unix(1_700_000_000, 0)
	if err := d.PublishPart(7, "sub/out.txt", 0o644, mtime); err != nil {
		t.Fatalf("publish: %v", err)
	}
	got, err := os.ReadFile(final)
	if err != nil || string(got) != "hello world" {
		t.Fatalf("final content = %q, %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".esync", "parts", "7.part")); !os.IsNotExist(err) {
		t.Error("part file survived publish")
	}
	fi, _ := os.Stat(final)
	if fi.Mode().Perm() != 0o644 {
		t.Errorf("mode = %v, want 0644", fi.Mode().Perm())
	}
	if !fi.ModTime().Equal(mtime) {
		t.Errorf("mtime = %v, want %v", fi.ModTime(), mtime)
	}
}

// §12.5: any symlink component on the destination path → E7013.
func TestSymlinkOnPathRejected(t *testing.T) {
	d, dir := newDest(t)
	if err := os.Mkdir(filepath.Join(dir, "real"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("real", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := d.EnsureDir("link/child", 0o755); fault.GetCode(err) != fault.E7013 {
		t.Fatalf("EnsureDir via symlink: got %v, want E7013", err)
	}

	f, _ := d.OpenPart(1)
	f.Close()
	if err := d.PublishPart(1, "link/x.txt", 0o644, time.Now()); fault.GetCode(err) != fault.E7013 {
		t.Fatalf("PublishPart via symlink: got %v, want E7013", err)
	}
}

// §12.5 / REQ-FS-043: an escaping or absolute symlink target → E7011.
func TestSymlinkTargetEscape(t *testing.T) {
	d, _ := newDest(t)
	if err := d.MakeSymlink("a/b/link", "../../../etc/passwd", false); fault.GetCode(err) != fault.E7011 {
		t.Fatalf("escaping target: got %v, want E7011", err)
	}
	if err := d.MakeSymlink("link2", "/etc/passwd", false); fault.GetCode(err) != fault.E7011 {
		t.Fatalf("absolute target: got %v, want E7011", err)
	}
	// a safe relative target is accepted, and --allow-unsafe-links bypasses the check
	if err := d.MakeSymlink("dir/link3", "../sibling", false); err != nil {
		t.Fatalf("safe target rejected: %v", err)
	}
	if err := d.MakeSymlink("link4", "/etc/hosts", true); err != nil {
		t.Fatalf("allowUnsafeLinks should bypass: %v", err)
	}
}

// §12.4 / REQ-FS-003: a failed hard link is E7009 (Warn), not fatal.
func TestHardlink(t *testing.T) {
	d, dir := newDest(t)
	if err := os.WriteFile(filepath.Join(dir, "orig.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := d.MakeHardlink("copy.txt", "orig.txt"); err != nil {
		t.Fatalf("hardlink: %v", err)
	}
	if err := d.MakeHardlink("bad.txt", "does-not-exist"); fault.GetCode(err) != fault.E7009 {
		t.Fatalf("failed hardlink: got %v, want E7009", err)
	}
}
