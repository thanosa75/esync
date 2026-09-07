package fault

import (
	"math/rand"
	"time"
)

// MaxRetriesDefault is the default value of --max-retries (ARCHITECTURE §14.3).
const MaxRetriesDefault = 3

const (
	backoffBase = 200 * time.Millisecond
	backoffCap  = 5 * time.Second
)

// Backoff returns the delay before retry attempt n (0-based), per ARCHITECTURE §14.3:
//
//	delay = min(200ms * 2^n, 5s) * jitter, jitter uniform in [0.5, 1.5)
//
// rnd must be a math/rand source (NOT crypto/rand). A nil rnd yields the
// un-jittered delay.
func Backoff(attempt int, rnd *rand.Rand) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	d := backoffCap
	if attempt < 24 {
		d = backoffBase << uint(attempt)
		if d <= 0 || d > backoffCap {
			d = backoffCap
		}
	}
	jitter := 1.0
	if rnd != nil {
		jitter = 0.5 + rnd.Float64() // [0.5, 1.5)
	}
	return time.Duration(float64(d) * jitter)
}
