package digest

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"esync/internal/fault"
	"esync/internal/obs"
)

// Digest cache (ARCHITECTURE §11.3). Advisory only: any problem reading it — a
// missing file, a bad header, a torn record — is a DEBUG log plus a one-time
// E8005 warning, and the run falls back to full hashing. It is never
// authoritative for correctness.
//
// File format: a fixed magic line, then a sequence of records:
//
//	key[keyLen] | digest_len u8 | digest[digest_len] | crc32 u32 (IEEE, over the preceding bytes of the record)
//
// A short or crc-mismatched trailing record is discarded on read.

const (
	cacheMagic = "esync digest cache v1\n"
	keyLen     = 8 + 8 + 8 + 8 + 8 + 8 + 1 // dev,ino,size,mtime_sec,mtime_nsec,ctime_sec,algo
	maxDigest  = 32
)

// CacheKey identifies a file version for the digest cache (ARCHITECTURE §11.3).
// ctime is included so an in-place modification that preserves mtime still misses.
type CacheKey struct {
	Dev       uint64
	Ino       uint64
	Size      uint64
	MtimeSec  int64
	MtimeNsec int64
	CtimeSec  int64
	Algo      Algo
}

// Cache is a process-wide digest cache. A nil *Cache is a valid no-op cache
// (used for --no-cache); all methods are nil-safe.
type Cache struct {
	ctx    obs.Ctx
	path   string
	mu     sync.Mutex
	m      map[CacheKey][]byte
	f      *os.File // append handle; nil when writing is disabled
	warned bool
}

// DefaultCachePath is ${XDG_CACHE_HOME:-~/.cache}/esync/digests.db.
func DefaultCachePath() (string, error) {
	if d := os.Getenv("XDG_CACHE_HOME"); d != "" {
		return filepath.Join(d, "esync", "digests.db"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fault.Wrap(fault.E8005, "resolve digest cache path", "", err)
	}
	return filepath.Join(home, ".cache", "esync", "digests.db"), nil
}

// OpenCache loads the cache at path. It never fails: a load problem yields an
// empty, read-only cache and a one-time E8005 warning on ctx.
func OpenCache(ctx obs.Ctx, path string) *Cache {
	c := &Cache{ctx: ctx, path: path, m: make(map[CacheKey][]byte)}
	if !c.load() {
		return c // load reported the problem; writing stays disabled
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		obs.Debug(ctx, "digest cache directory unavailable", obs.F("err", err.Error()))
		return c
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		obs.Debug(ctx, "digest cache not writable", obs.F("err", err.Error()))
		return c
	}
	if fi, err := f.Stat(); err == nil && fi.Size() == 0 {
		if _, err := f.WriteString(cacheMagic); err != nil {
			_ = f.Close()
			return c
		}
	}
	c.f = f
	return c
}

// load populates c.m from disk. It returns true when the file was absent or
// parsed cleanly (writing may be enabled), false when it was unusable.
func (c *Cache) load() bool {
	data, err := os.ReadFile(c.path)
	if errors.Is(err, fs.ErrNotExist) {
		return true
	}
	if err != nil {
		c.warn("cannot read digest cache", err)
		return false
	}
	if len(data) < len(cacheMagic) || string(data[:len(cacheMagic)]) != cacheMagic {
		c.warn("digest cache header not recognised", nil)
		return false
	}
	p := data[len(cacheMagic):]
	for len(p) > 0 {
		if len(p) < keyLen+1 {
			c.warn("digest cache has a torn trailing record", nil)
			return true
		}
		dl := int(p[keyLen])
		recLen := keyLen + 1 + dl + 4
		if dl > maxDigest || len(p) < recLen {
			c.warn("digest cache has a torn trailing record", nil)
			return true
		}
		want := binary.BigEndian.Uint32(p[keyLen+1+dl : recLen])
		if crc32.ChecksumIEEE(p[:keyLen+1+dl]) != want {
			c.warn("digest cache has a torn trailing record", nil)
			return true
		}
		key := decodeKey(p[:keyLen])
		c.m[key] = append([]byte(nil), p[keyLen+1:keyLen+1+dl]...)
		p = p[recLen:]
	}
	return true
}

func (c *Cache) warn(what string, err error) {
	obs.Debug(c.ctx, "digest cache unusable", obs.F("detail", what), obs.F("err", errText(err)))
	if !c.warned {
		c.warned = true
		obs.LogFault(c.ctx, fault.Newf(fault.E8005, what, c.path, err, "using full hashing"))
	}
}

// Get returns a cached digest for key, if present.
func (c *Cache) Get(key CacheKey) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	d, ok := c.m[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), d...), true
}

// Put records digest for key. A write error disables further writes for the run
// (advisory only); it is never surfaced to the caller.
func (c *Cache) Put(key CacheKey, digest []byte) {
	if c == nil || len(digest) == 0 || len(digest) > maxDigest {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.m[key]; ok {
		return
	}
	c.m[key] = append([]byte(nil), digest...)
	if c.f == nil {
		return
	}
	rec := make([]byte, 0, keyLen+1+len(digest)+4)
	rec = appendKey(rec, key)
	rec = append(rec, byte(len(digest)))
	rec = append(rec, digest...)
	rec = binary.BigEndian.AppendUint32(rec, crc32.ChecksumIEEE(rec))
	if _, err := c.f.Write(rec); err != nil {
		obs.Debug(c.ctx, "digest cache write failed", obs.F("err", err.Error()))
		_ = c.f.Close()
		c.f = nil
	}
}

// Close releases the append handle.
func (c *Cache) Close() error {
	if c == nil || c.f == nil {
		return nil
	}
	err := c.f.Close()
	c.f = nil
	return err
}

func appendKey(b []byte, k CacheKey) []byte {
	b = binary.BigEndian.AppendUint64(b, k.Dev)
	b = binary.BigEndian.AppendUint64(b, k.Ino)
	b = binary.BigEndian.AppendUint64(b, k.Size)
	b = binary.BigEndian.AppendUint64(b, uint64(k.MtimeSec))
	b = binary.BigEndian.AppendUint64(b, uint64(k.MtimeNsec))
	b = binary.BigEndian.AppendUint64(b, uint64(k.CtimeSec))
	return append(b, byte(k.Algo))
}

func decodeKey(b []byte) CacheKey {
	return CacheKey{
		Dev:       binary.BigEndian.Uint64(b[0:8]),
		Ino:       binary.BigEndian.Uint64(b[8:16]),
		Size:      binary.BigEndian.Uint64(b[16:24]),
		MtimeSec:  int64(binary.BigEndian.Uint64(b[24:32])),
		MtimeNsec: int64(binary.BigEndian.Uint64(b[32:40])),
		CtimeSec:  int64(binary.BigEndian.Uint64(b[40:48])),
		Algo:      Algo(b[48]),
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
