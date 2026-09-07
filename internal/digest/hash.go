package digest

import (
	"errors"
	"io"
	"io/fs"
	"os"

	"esync/internal/fault"
	"esync/internal/obs"
)

// hashBuf is the streaming buffer size (ARCHITECTURE §11.1): a huge file costs no
// more memory than a small one.
const hashBuf = 256 * 1024

// HashFile streams path through algo and returns its digest, using cache as an
// advisory shortcut. Memory use is constant regardless of file size. ctx is
// honoured for cancellation between buffers.
func HashFile(ctx obs.Ctx, path string, algo Algo, cache *Cache) ([]byte, error) {
	h, err := New(algo)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, openFault(path, err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, fault.Wrap(fault.E6004, "stat source file", path, err)
	}

	var key CacheKey
	haveKey := false
	if cache != nil {
		if k, ok := statKey(fi, algo); ok {
			key, haveKey = k, true
			if d, hit := cache.Get(key); hit {
				ctx.Counters().CacheHit.Add(1)
				obs.Trace(ctx, "digest cache hit", obs.F("path", path))
				return d, nil
			}
			ctx.Counters().CacheMiss.Add(1)
		}
	}

	buf := make([]byte, hashBuf)
	for {
		if ctx.Context != nil && ctx.Err() != nil {
			return nil, fault.Wrap(fault.E6004, "hash source file", path, ctx.Err())
		}
		n, rerr := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return nil, fault.Wrap(fault.E6004, "read source file", path, rerr)
		}
	}

	sum := h.Sum(nil)
	if haveKey {
		cache.Put(key, sum)
	}
	return sum, nil
}

func openFault(path string, err error) error {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fault.Wrap(fault.E6002, "open source file", path, err)
	case errors.Is(err, fs.ErrPermission):
		return fault.Wrap(fault.E6001, "open source file", path, err)
	default:
		return fault.Wrap(fault.E6004, "open source file", path, err)
	}
}
