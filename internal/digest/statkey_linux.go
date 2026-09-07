//go:build linux

package digest

import (
	"io/fs"
	"syscall"
)

// statKey builds a CacheKey from a stat result (ARCHITECTURE §11.3). The second
// return is false when the platform stat is unavailable, in which case the
// caller hashes without consulting the cache.
func statKey(fi fs.FileInfo, algo Algo) (CacheKey, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return CacheKey{}, false
	}
	return CacheKey{
		Dev:       uint64(st.Dev),
		Ino:       st.Ino,
		Size:      uint64(fi.Size()),
		MtimeSec:  int64(st.Mtim.Sec),
		MtimeNsec: int64(st.Mtim.Nsec),
		CtimeSec:  int64(st.Ctim.Sec),
		Algo:      algo,
	}, true
}
