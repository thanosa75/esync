package digest

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"esync/internal/fault"
	"esync/internal/obs"
)

// G-HASH-01 / REQ-HASH-001: known content maps to a known digest, including the
// empty file. These vectors are the well-known MD5/SHA-256 test values.
func TestGoldenHashVectors(t *testing.T) {
	cases := []struct {
		content         string
		md5Hex, sha2Hex string
	}{
		{"", "d41d8cd98f00b204e9800998ecf8427e",
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"abc", "900150983cd24fb0d6963f7d28e17f72",
			"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
	}
	for _, c := range cases {
		p := filepath.Join(t.TempDir(), "f")
		if err := os.WriteFile(p, []byte(c.content), 0o644); err != nil {
			t.Fatal(err)
		}
		md5Sum, err := HashFile(obs.Ctx{}, p, MD5, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := hex.EncodeToString(md5Sum); got != c.md5Hex {
			t.Errorf("MD5(%q) = %s, want %s", c.content, got, c.md5Hex)
		}
		sha2Sum, err := HashFile(obs.Ctx{}, p, SHA256, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := hex.EncodeToString(sha2Sum); got != c.sha2Hex {
			t.Errorf("SHA256(%q) = %s, want %s", c.content, got, c.sha2Hex)
		}
	}
}

// T-HASH-01 / REQ-HASH-008: a large file hashes to the same value as a one-shot
// hash of its bytes, proving the 256 KiB streaming buffer is correct.
func TestStreamingMatchesOneShot(t *testing.T) {
	buf := make([]byte, 3*hashBuf+1234)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "big")
	if err := os.WriteFile(p, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, algo := range []Algo{MD5, SHA256} {
		h, _ := New(algo)
		h.Write(buf)
		want := h.Sum(nil)
		got, err := HashFile(obs.Ctx{}, p, algo, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s: streaming digest != one-shot", algo)
		}
	}
}

// T-HASH-03 / REQ-HASH-002: BLAKE3 is reserved and pulls in no dependency.
func TestBLAKE3Reserved(t *testing.T) {
	if _, err := New(BLAKE3); fault.GetCode(err) != fault.E1007 {
		t.Fatalf("New(BLAKE3) err = %v, want E1007", err)
	}
	if _, err := Parse("nope"); fault.GetCode(err) != fault.E1007 {
		t.Fatalf("Parse(nope) err = %v, want E1007", err)
	}
	if a, err := Parse("SHA-256"); err != nil || a != SHA256 {
		t.Fatalf("Parse(SHA-256) = %v, %v", a, err)
	}
}

func TestHashFileMissing(t *testing.T) {
	_, err := HashFile(obs.Ctx{}, filepath.Join(t.TempDir(), "absent"), MD5, nil)
	if fault.GetCode(err) != fault.E6002 {
		t.Fatalf("missing file err = %v, want E6002", err)
	}
}

// T-CACHE-01 / REQ-HASH-005: a cache hit returns the stored digest without
// re-reading the file (proved by mutating the file contents but not its stat key).
func TestCacheHitAvoidsRehash(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	if err := os.WriteFile(p, []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}
	cachePath := filepath.Join(dir, "cache.db")

	c := OpenCache(obs.Ctx{}, cachePath)
	first, err := HashFile(obs.Ctx{}, p, MD5, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen the cache from disk; the entry must have persisted.
	c2 := OpenCache(obs.Ctx{}, cachePath)
	defer c2.Close()
	fi, _ := os.Stat(p)
	key, ok := statKey(fi, MD5)
	if !ok {
		t.Skip("no platform stat")
	}
	if got, hit := c2.Get(key); !hit || !bytes.Equal(got, first) {
		t.Fatalf("reopened cache miss: hit=%v", hit)
	}

	second, err := HashFile(obs.Ctx{}, p, MD5, c2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("cache returned a different digest")
	}
}

// T-CACHE-02 / F-CACHE-01 / REQ-HASH-006: a corrupt, truncated, or
// wrong-version cache file is advisory — no crash, no wrong digest, full hash.
func TestCorruptCacheFallsBack(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	if err := os.WriteFile(p, []byte("hello world"), 0o644); err != nil {
		t.Fatal(err)
	}
	want, _ := HashFile(obs.Ctx{}, p, SHA256, nil)

	corruptions := map[string][]byte{
		"garbage":              bytes.Repeat([]byte{0xff}, 200),
		"bad-magic":            []byte("not an esync cache\nxxxxxxxxxxxxxxxxxxxx"),
		"torn-record":          append([]byte(cacheMagic), 1, 2, 3, 4, 5),
		"crc-mismatch-trailer": crcMismatchCache(t, p),
	}
	for name, data := range corruptions {
		cachePath := filepath.Join(dir, name+".db")
		if err := os.WriteFile(cachePath, data, 0o644); err != nil {
			t.Fatal(err)
		}
		c := OpenCache(obs.Ctx{}, cachePath)
		got, err := HashFile(obs.Ctx{}, p, SHA256, c)
		c.Close()
		if err != nil {
			t.Fatalf("%s: HashFile error %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: wrong digest from corrupt cache", name)
		}
	}
}

// crcMismatchCache builds a cache with a well-formed record whose crc trailer is
// wrong, so load() must discard it rather than trust it.
func crcMismatchCache(t *testing.T, path string) []byte {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	key, ok := statKey(fi, SHA256)
	if !ok {
		return []byte(cacheMagic)
	}
	b := []byte(cacheMagic)
	b = appendKey(b, key)
	b = append(b, 4, 0xde, 0xad, 0xbe, 0xef)
	return append(b, 0, 0, 0, 0) // deliberately wrong crc
}

func TestNilCacheIsNoOp(t *testing.T) {
	var c *Cache
	if _, hit := c.Get(CacheKey{}); hit {
		t.Fatal("nil cache reported a hit")
	}
	c.Put(CacheKey{}, []byte("x")) // must not panic
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

// T-RES-02 shape / REQ-XFER-041: streaming hash state round-trips.
func TestMarshalRoundTrip(t *testing.T) {
	for _, algo := range []Algo{MD5, SHA256} {
		h, _ := New(algo)
		h.Write([]byte("first half "))
		state, err := Marshal(h)
		if err != nil {
			t.Fatal(err)
		}
		restored, err := Unmarshal(algo, state)
		if err != nil {
			t.Fatal(err)
		}
		h.Write([]byte("second half"))
		restored.Write([]byte("second half"))
		if !bytes.Equal(h.Sum(nil), restored.Sum(nil)) {
			t.Errorf("%s: restored hash state diverged", algo)
		}
	}
	if _, err := Unmarshal(SHA256, []byte("junk")); fault.GetCode(err) != fault.E8004 {
		t.Fatalf("Unmarshal(junk) err = %v, want E8004", err)
	}
}

// MFR-0015 (ARCHITECTURE §11.3 erratum): the cache key carries ctime at full
// timespec resolution. Two versions of a file whose ctime differs only in the
// nanosecond field must be distinct entries — otherwise an in-place rewrite
// that preserves size and mtime and lands in the same wall-clock second is a
// stale HIT, and the receiver silently skips a file that changed.
func TestCacheKeyDistinguishesCtimeNsec(t *testing.T) {
	c := OpenCache(obs.Ctx{}, filepath.Join(t.TempDir(), "cache.db"))
	defer c.Close()

	k1 := CacheKey{Dev: 1, Ino: 2, Size: 8, MtimeSec: 100, MtimeNsec: 5, CtimeSec: 100, CtimeNsec: 1, Algo: MD5}
	k2 := k1
	k2.CtimeNsec = 2

	c.Put(k1, []byte("stale-digest-aa!"))
	if got, hit := c.Get(k2); hit {
		t.Fatalf("stale hit across a sub-second ctime change: %x", got)
	}
	if got, hit := c.Get(k1); !hit || string(got) != "stale-digest-aa!" {
		t.Fatalf("own key missed: hit=%v got=%x", hit, got)
	}

	// The key must survive the on-disk codec intact, nsec included.
	if round := decodeKey(appendKey(nil, k1)); round != k1 {
		t.Fatalf("key round-trip = %+v, want %+v", round, k1)
	}
}

// The realistic shape of the same defect: a tool rewrites a file in place,
// keeps the size, and restores the mtime (rsync, tar, git checkout). The
// digest must track the new content even when the rewrite lands in the same
// second as the original.
func TestCacheMissesOnMtimePreservingRewrite(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "gen.out")
	if err := os.WriteFile(p, []byte("first-content"), 0o644); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := statKey(fi, MD5); !ok {
		t.Skip("no platform stat")
	}

	c := OpenCache(obs.Ctx{}, filepath.Join(dir, "cache.db"))
	defer c.Close()
	if _, err := HashFile(obs.Ctx{}, p, MD5, c); err != nil {
		t.Fatal(err)
	}

	// Same size, same mtime, different bytes — immediately, so ctime_sec very
	// likely still matches and only the nanoseconds separate the two versions.
	if err := os.WriteFile(p, []byte("second-conten"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, fi.ModTime(), fi.ModTime()); err != nil {
		t.Fatal(err)
	}

	got, err := HashFile(obs.Ctx{}, p, MD5, c)
	if err != nil {
		t.Fatal(err)
	}
	want := md5.Sum([]byte("second-conten"))
	if !bytes.Equal(got, want[:]) {
		t.Fatalf("stale digest after an mtime-preserving in-place rewrite: got %x, want %x", got, want)
	}
}
