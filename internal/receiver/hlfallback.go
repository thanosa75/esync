package receiver

import (
	"sync/atomic"

	"esync/internal/obs"
)

// hlFallbackDeps is the dependency set of the hardlink content-fallback path
// (§12.4 / R-16): fetching a secondary's content because MakeHardlink failed.
// Two callers reach it — decide, when the primary was resolved by a SKIP, and
// fetch, when the primary was just published — and both go through
// fetchInstead so their space reservation and progress accounting cannot drift
// apart. They did in the first version of this fix: the fetch-side copy
// reserved no space and never grew the progress denominator, so a fallback
// wrote content past the §12.7 free-space guard and could drive the progress
// bar over 100%.
type hlFallbackDeps struct {
	octx        obs.Ctx
	meta        *metaStore
	guard       *spaceGuard
	q           *needQueue
	counters    *obs.Counters
	neededBytes *atomic.Int64
	neededFiles *atomic.Int64
}

// fetchInstead enqueues fileID/rel as an ordinary needed file under groupID
// (the group being processed now, not the secondary's own group, whose
// GROUP_DECISION went out long ago). Every failure here is the last-resort
// safety net: the file counts as failed rather than vanishing from the
// destination on an exit-0 run.
func (d hlFallbackDeps) fetchInstead(groupID uint32, fileID uint64, rel string) {
	fm, ok := d.meta.get(fileID)
	if !ok {
		obs.Warn(d.octx, "hardlink fallback: no metadata for secondary, counting as failed", obs.F("path", rel))
		d.counters.FilesFailed.Add(1)
		return
	}
	if d.guard != nil {
		warn, ferr := d.guard.reserve(fm.size)
		if warn {
			obs.Warn(d.octx, "destination free space is low", obs.F("group", groupID))
		}
		if ferr != nil {
			obs.LogFault(d.octx, ferr)
			d.counters.FilesFailed.Add(1)
			return
		}
	}
	if d.neededBytes != nil {
		d.neededBytes.Add(fm.size)
		d.neededFiles.Add(1)
	}
	d.q.pushFallback(needItem{fileID: fileID, groupID: groupID, size: fm.size, rel: rel})
}
