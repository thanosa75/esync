package receiver

import (
	"time"

	"esync/internal/obs"
)

// Tuning constants (ARCHITECTURE §13.4).
const (
	tuneWindow          = 2 * time.Second
	tuneRampUpThreshold = 1.08 // ramp up once goodput beats the best seen by >8%
	tuneRampDnThreshold = 0.92 // ramp down once goodput falls below the best seen by >8%
	tuneRampUpStep      = 2
	tuneMaxUnproductive = 3 // consecutive ramps that don't raise the best goodput => stop for good
)

type rampAction int

const (
	rampHold rampAction = iota
	rampUp
	rampDown
)

// decideRamp is the pure §13.4 decision for one 2 s window: given the current
// channel count, its bounds, the goodput measured this window, the best
// goodput seen so far, and whether an error was observed this window, it
// returns what to do and the best-goodput to carry forward. It is kept free of
// I/O and goroutines so the ramp/hysteresis logic is unit-testable without real
// channels.
func decideRamp(n, minCh, maxCh int, g, bestG float64, errored bool) (action rampAction, newBestG float64) {
	switch {
	case !errored && n < maxCh && g > bestG*tuneRampUpThreshold:
		return rampUp, max(bestG, g)
	case n > minCh && g < bestG*tuneRampDnThreshold:
		return rampDown, bestG
	default:
		return rampHold, bestG
	}
}

// tune is the adaptive channel-count controller (ARCHITECTURE §13.4,
// REQ-PAR-004). Every 2 s it samples aggregate goodput from the live byte
// counter, ramps N up by 2 (opening new CHANNEL_JOIN connections, §7.1) while
// goodput keeps beating its best and the window was error-free, ramps N down
// by 1 (retiring the most recently opened, presumed-idlest channel) once
// goodput regresses, and otherwise holds. Three ramps in a row that fail to
// raise the best goodput stop the controller for good (hysteresis), so a noisy
// link settles instead of oscillating (RISK-05). --channels pins N and the
// call site in Run never starts this goroutine in that case; the check here is
// a second guard for direct callers (e.g. tests).
func (s *session) tune(octx obs.Ctx) {
	if s.cfg.ChannelsPinned {
		return
	}

	minCh, maxCh := s.cfg.MinChannels, s.cfg.MaxChannels
	if s.senderMaxChannels > 0 && s.senderMaxChannels < maxCh {
		maxCh = s.senderMaxChannels
	}
	if maxCh < minCh {
		maxCh = minCh
	}

	ticker := time.NewTicker(tuneWindow)
	defer ticker.Stop()

	var (
		bestG        float64
		lastBytes    = s.cnt.Bytes.Load()
		lastRetries  = s.cnt.Retries.Load()
		lastFailed   = s.cnt.FilesFailed.Load()
		unproductive int
		stopped      bool
	)

	for {
		select {
		case <-s.rootCtx.Done():
			return
		case <-ticker.C:
		}

		bytesNow, retriesNow, failedNow := s.cnt.Bytes.Load(), s.cnt.Retries.Load(), s.cnt.FilesFailed.Load()
		g := float64(bytesNow-lastBytes) / tuneWindow.Seconds()
		errored := retriesNow > lastRetries || failedNow > lastFailed
		lastBytes, lastRetries, lastFailed = bytesNow, retriesNow, failedNow

		if stopped {
			obs.Trace(octx, "adaptive tuner hold (settled)", obs.F("goodput_bps", int64(g)))
			continue
		}

		n := s.channelCount()
		if n == 0 {
			continue // pipeline not yet up, or already draining
		}

		action, newBestG := decideRamp(n, minCh, maxCh, g, bestG, errored)
		bestG = newBestG

		switch action {
		case rampUp:
			target := min(n+tuneRampUpStep, maxCh)
			opened := 0
			for i := n; i < target; i++ {
				if err := s.addChannel(octx); err != nil {
					if err != errPipelineDone {
						obs.Warn(octx, "adaptive tuner: could not open a channel", obs.F("err", err.Error()))
					}
					break
				}
				opened++
			}
			unproductive = 0
			if opened > 0 {
				obs.Debug(octx, "adaptive tuner: ramp up",
					obs.F("goodput_bps", int64(g)), obs.F("from", n), obs.F("to", n+opened))
			}
		case rampDown:
			if s.retireOne() {
				unproductive++
				obs.Debug(octx, "adaptive tuner: ramp down",
					obs.F("goodput_bps", int64(g)), obs.F("from", n), obs.F("to", n-1))
			}
		default:
			obs.Trace(octx, "adaptive tuner: hold", obs.F("goodput_bps", int64(g)), obs.F("channels", n))
		}

		if unproductive >= tuneMaxUnproductive {
			stopped = true
			obs.Debug(octx, "adaptive tuner: settled (hysteresis)", obs.F("channels", s.channelCount()))
		}
	}
}
