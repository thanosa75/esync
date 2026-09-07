package sender

import (
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"esync/internal/fault"
	"esync/internal/obs"
	"esync/internal/plan"
)

func collectWalk(t *testing.T, cfg walkConfig) ([]plan.Entry, uint32, error) {
	t.Helper()
	ch := make(chan plan.Entry, 256)
	var out []plan.Entry
	var skipped uint32
	var werr error
	done := make(chan struct{})
	go func() {
		skipped, werr = walkTree(obs.Ctx{}, cfg, ch)
		close(ch)
		close(done)
	}()
	for e := range ch {
		out = append(out, e)
	}
	<-done
	sort.Slice(out, func(i, j int) bool { return string(out[i].RelPath) < string(out[j].RelPath) })
	return out, skipped, werr
}

func relPaths(es []plan.Entry) []string {
	s := make([]string, len(es))
	for i, e := range es {
		s[i] = filepath.ToSlash(string(e.RelPath))
	}
	return s
}

// T-SCAN-01: a plain tree enumerates every file and directory, relative to root.
func TestWalkBasicEnumeration(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), []byte("aaa"))
	writeFile(t, filepath.Join(root, "sub", "b.txt"), []byte("bbbb"))
	writeFile(t, filepath.Join(root, "sub", "deep", "c.txt"), []byte("c"))

	es, skipped, err := collectWalk(t, walkConfig{root: root})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if skipped != 0 {
		t.Fatalf("skipped = %d, want 0", skipped)
	}
	got := relPaths(es)
	want := []string{"a.txt", "sub", "sub/b.txt", "sub/deep", "sub/deep/c.txt"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entry %d = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
	for _, e := range es {
		if filepath.ToSlash(string(e.RelPath)) == "a.txt" {
			if e.Type != plan.TypeFile || e.Size != 3 {
				t.Fatalf("a.txt entry wrong: %+v", e)
			}
		}
	}
}

// T-SCAN-07: a symlink is recorded as a symlink, its target is not followed, and
// .esync is always skipped.
func TestWalkSymlinkRecordedNotFollowed(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks")
	}
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "real", "x.txt"), []byte("x"))
	if err := os.Symlink(filepath.Join(root, "real"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(root, ".esync", "journal"), []byte("j"))

	es, _, err := collectWalk(t, walkConfig{root: root})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	var sawLink bool
	for _, e := range es {
		p := filepath.ToSlash(string(e.RelPath))
		if p == "link" {
			sawLink = true
			if e.Type != plan.TypeSymlink {
				t.Fatalf("link recorded as type %d, want symlink", e.Type)
			}
			if len(e.LinkTarget) == 0 {
				t.Fatal("link target not recorded")
			}
		}
		if p == "link/x.txt" {
			t.Fatal("symlink was followed")
		}
		if p == ".esync" || p == ".esync/journal" {
			t.Fatal(".esync was not skipped")
		}
	}
	if !sawLink {
		t.Fatal("symlink entry missing")
	}
}

// T-SCAN-08: an unreadable directory is skipped with a warning, and the rest of
// the tree still enumerates.
func TestWalkUnreadableDirectorySkipped(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root defeats the permission bit")
	}
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "ok.txt"), []byte("ok"))
	secret := filepath.Join(root, "locked")
	writeFile(t, filepath.Join(secret, "hidden.txt"), []byte("h"))
	if err := os.Chmod(secret, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(secret, 0o755) })

	es, skipped, err := collectWalk(t, walkConfig{root: root})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if skipped != 1 {
		t.Fatalf("skipped = %d, want 1", skipped)
	}
	for _, e := range es {
		if filepath.ToSlash(string(e.RelPath)) == "locked/hidden.txt" {
			t.Fatal("read a file inside an unreadable directory")
		}
	}
}

// T-SCAN-09: a special file (FIFO) is skipped, counted, and not enumerated.
func TestWalkSpecialFileSkipped(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "plain.txt"), []byte("p"))
	fifo := filepath.Join(root, "pipe")
	if err := mkfifo(fifo); err != nil {
		t.Skipf("mkfifo unsupported: %v", err)
	}

	es, skipped, err := collectWalk(t, walkConfig{root: root})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if skipped != 1 {
		t.Fatalf("skipped = %d, want 1", skipped)
	}
	for _, e := range es {
		if filepath.ToSlash(string(e.RelPath)) == "pipe" {
			t.Fatal("FIFO was enumerated")
		}
	}
}

// A missing source root is a fatal E1003.
func TestWalkMissingRoot(t *testing.T) {
	_, _, err := collectWalk(t, walkConfig{root: filepath.Join(t.TempDir(), "nope")})
	if fault.GetCode(err) != fault.E1003 {
		t.Fatalf("code = %v, want E1003", fault.GetCode(err))
	}
}
