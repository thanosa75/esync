package obs

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

// defaultHeartbeatSample and defaultHeartbeatLog are the production cadences:
// bandwidth is resampled every 10s (so the reported rate always reflects the
// trailing 10s window, not the whole session) and logged every 60s.
const (
	defaultHeartbeatSample = 10 * time.Second
	defaultHeartbeatLog    = 60 * time.Second
)

// Heartbeat periodically logs which groups are still in progress, how many
// data channels are live, and the transfer rate averaged over the trailing
// sample window. A per-group DEBUG line already exists (group.decide), but
// nothing at INFO level says which groups are in flight *right now* on a
// long-running, multi-channel transfer.
type Heartbeat struct {
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// NewHeartbeat starts the background logger. groups returns the currently
// in-progress group ids (ascending, may be empty); channels returns the live
// data-channel count. sampleInterval <= 0 defaults to 10s, logInterval <= 0
// defaults to 60s (and is rounded up to the next multiple of the sample
// interval). Stop must be called once the transfer ends.
func NewHeartbeat(ctx Ctx, sampleInterval, logInterval time.Duration, groups func() []uint32, channels func() int) *Heartbeat {
	if sampleInterval <= 0 {
		sampleInterval = defaultHeartbeatSample
	}
	if logInterval <= 0 {
		logInterval = defaultHeartbeatLog
	}
	samplesPerLog := int(logInterval / sampleInterval)
	if samplesPerLog < 1 {
		samplesPerLog = 1
	}
	h := &Heartbeat{stop: make(chan struct{}), done: make(chan struct{})}
	Go(ctx, "heartbeat", func() error {
		h.loop(ctx, sampleInterval, samplesPerLog, groups, channels)
		close(h.done)
		return nil
	})
	return h
}

// Stop halts the background goroutine and waits for it to exit, so nothing it
// might still be doing (like a final log write) races the caller. Idempotent.
func (h *Heartbeat) Stop() {
	h.once.Do(func() { close(h.stop) })
	<-h.done
}

func (h *Heartbeat) loop(ctx Ctx, sampleInterval time.Duration, samplesPerLog int, groups func() []uint32, channels func() int) {
	c := ctx.Counters()
	t := time.NewTicker(sampleInterval)
	defer t.Stop()

	lastBytes := c.Bytes.Load()
	lastAt := time.Now()
	var rate float64
	ticks := 0
	for {
		select {
		case <-h.stop:
			return
		case now := <-t.C:
			b := c.Bytes.Load()
			if elapsed := now.Sub(lastAt).Seconds(); elapsed > 0 {
				rate = float64(b-lastBytes) / elapsed
			}
			lastBytes, lastAt = b, now
			ticks++
			if ticks%samplesPerLog == 0 {
				Info(ctx, "groups in progress",
					F("groups", formatGroups(groups())),
					F("channels", channels()),
					F("rate", humanRate(rate)))
			}
		}
	}
}

func formatGroups(ids []uint32) string {
	if len(ids) == 0 {
		return "none"
	}
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatUint(uint64(id), 10)
	}
	return strings.Join(parts, ",")
}
