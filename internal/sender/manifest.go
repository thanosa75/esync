package sender

import (
	"context"
	"path/filepath"
	"sync"

	"esync/internal/channel"
	"esync/internal/digest"
	"esync/internal/obs"
	"esync/internal/plan"
	"esync/internal/wire"
)

// manifestPublisher runs the scan/hash/publish pipeline (ARCHITECTURE §11.1):
// it emits SCAN_COMPLETE, then publishes one GROUP_MANIFEST per group, hashing
// a group's files only once credit allows that group to go out (lazy hashing,
// T-PAR-06).
type manifestPublisher struct {
	ctx     obs.Ctx
	ctrl    *channel.Conn
	pl      *plan.Plan
	root    string
	algo    digest.Algo
	cache   *digest.Cache
	quick   bool
	workers int
	skipped uint32
	digests *digestStore
	credit  *creditGate
}

func (mp *manifestPublisher) run(ctx context.Context) error {
	dg := mp.pl.ManifestDigest()
	if err := mp.ctrl.SendMsg(&wire.ScanComplete{
		TotalFiles:     mp.pl.TotalFiles,
		TotalBytes:     mp.pl.TotalBytes,
		TotalGroups:    uint32(mp.pl.NumGroups()),
		SkippedEntries: mp.skipped,
		ManifestDigest: dg[:],
	}); err != nil {
		return err
	}
	obs.Info(mp.ctx, "scan complete",
		obs.F("files", mp.pl.TotalFiles), obs.F("bytes", mp.pl.TotalBytes), obs.F("groups", mp.pl.NumGroups()))

	for gi, entries := range mp.pl.Groups() {
		if err := mp.credit.acquire(ctx); err != nil {
			return err
		}
		if err := mp.publishGroup(ctx, gi, entries); err != nil {
			return err
		}
	}
	return nil
}

func (mp *manifestPublisher) publishGroup(ctx context.Context, gi int, entries []plan.Entry) error {
	end := obs.Start(mp.ctx, "group.publish")
	firstID := uint64(gi) * groupEntries
	gm := &wire.GroupManifest{
		GroupID:     uint32(gi),
		FirstFileID: firstID,
		Entries:     make([]wire.ManifestEntry, len(entries)),
	}

	sem := make(chan struct{}, mp.workers)
	var wg sync.WaitGroup

	for i := range entries {
		e := entries[i]
		me := wire.ManifestEntry{
			EntryType: uint8(e.Type),
			Flags:     uint16(e.Flags),
			Path:      toSlashBytes(e.RelPath),
			Size:      uint64(e.Size),
			Mode:      uint32(e.Mode.Perm()),
			MtimeSec:  e.MtimeSec,
			MtimeNsec: e.MtimeNsec,
		}
		if e.Type == plan.TypeSymlink {
			me.LinkTarget = append([]byte(nil), e.LinkTarget...)
		}
		if e.Flags&plan.FlagHasHardlinkKey != 0 {
			me.HardlinkKey = e.HardlinkKey
		}
		gm.Entries[i] = me

		if e.Type != plan.TypeFile || mp.quick || e.Size == 0 {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		idx, fileID := i, firstID+uint64(i)
		abs := filepath.Join(mp.root, filepath.FromSlash(string(gm.Entries[i].Path)))
		obs.Go(mp.ctx, "group.hash", func() error {
			defer wg.Done()
			defer func() { <-sem }()
			d, err := digest.HashFile(mp.ctx, abs, mp.algo, mp.cache)
			if err != nil {
				obs.LogFault(mp.ctx, err)
				return nil
			}
			mp.digests.set(fileID, d)
			gm.Entries[idx].Digest = d
			return nil
		})
	}
	wg.Wait()

	if err := mp.ctrl.SendMsg(gm); err != nil {
		end("error")
		return err
	}
	end("ok", obs.F("group", gi), obs.F("entries", len(entries)))
	return nil
}

// toSlashBytes converts an OS-separated relative path to the '/'-separated form
// the wire manifest carries.
func toSlashBytes(rel []byte) []byte {
	out := append([]byte(nil), rel...)
	if filepath.Separator == '/' {
		return out
	}
	for i, c := range out {
		if c == filepath.Separator {
			out[i] = '/'
		}
	}
	return out
}
