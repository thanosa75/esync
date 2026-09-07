package receiver

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"esync/internal/obs"
)

// Pure §13.4 decision table: decideRamp must reproduce the pseudocode exactly,
// independent of any real channel/goroutine machinery.
func TestDecideRamp(t *testing.T) {
	cases := []struct {
		name            string
		n, minCh, maxCh int
		g, bestG        float64
		errored         bool
		wantAction      rampAction
		wantBestG       float64
	}{
		{
			name: "first window ramps up from zero best", n: 4, minCh: 4, maxCh: 32,
			g: 1000, bestG: 0, errored: false, wantAction: rampUp, wantBestG: 1000,
		},
		{
			name: "within the +-8% band holds", n: 6, minCh: 4, maxCh: 32,
			g: 1050, bestG: 1000, errored: false, wantAction: rampHold, wantBestG: 1000,
		},
		{
			name: "regression beyond -8% ramps down", n: 6, minCh: 4, maxCh: 32,
			g: 900, bestG: 1000, errored: false, wantAction: rampDown, wantBestG: 1000,
		},
		{
			name: "an errored window never ramps up even on a goodput jump", n: 4, minCh: 4, maxCh: 32,
			g: 2000, bestG: 1000, errored: true, wantAction: rampHold, wantBestG: 1000,
		},
		{
			name: "at the ceiling holds instead of ramping up further", n: 32, minCh: 4, maxCh: 32,
			g: 2000, bestG: 1000, errored: false, wantAction: rampHold, wantBestG: 1000,
		},
		{
			name: "at the floor holds instead of ramping down further", n: 4, minCh: 4, maxCh: 32,
			g: 100, bestG: 1000, errored: false, wantAction: rampHold, wantBestG: 1000,
		},
		{
			name: "exactly at the up threshold does not ramp (strictly greater required)", n: 4, minCh: 4, maxCh: 32,
			g: 1080, bestG: 1000, errored: false, wantAction: rampHold, wantBestG: 1000,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			action, bestG := decideRamp(c.n, c.minCh, c.maxCh, c.g, c.bestG, c.errored)
			if action != c.wantAction {
				t.Errorf("action = %v, want %v", action, c.wantAction)
			}
			if bestG != c.wantBestG {
				t.Errorf("newBestG = %v, want %v", bestG, c.wantBestG)
			}
		})
	}
}

// T-PAR-04-ish: a real (paced) transfer that spans more than one 2s tuning
// window must drive the live controller to CHANNEL_JOIN additional data
// channels beyond the floor it started at (ARCHITECTURE §13.4, REQ-PAR-004),
// and the sender must accept them (§7.1) — this exercises both halves of the
// wire-level ramp-up path, not just the pure decision function above.
func TestRunAdaptiveRampUp(t *testing.T) {
	if testing.Short() {
		t.Skip("paced transfer takes several seconds")
	}
	const (
		fileSize  = 3 << 20 // 3 MiB
		chunkSize = 16 << 10
		chunkGap  = 20 * time.Millisecond // ~192 chunks * 20ms ~= 3.8s, comfortably > one 2s tuning window
	)
	data := mkbytes(fileSize, 0x11)
	files := []treeFile{{rel: "big.bin", data: data}}
	fs := newFakeSender(t, files)
	fs.chunkSize = chunkSize
	fs.chunkDelay = chunkGap
	dest := t.TempDir()

	octx, flush := obs.Init(obs.Config{Sync: true, Role: "receiver"})
	defer flush()
	sum, exit := Run(octx, Config{
		Link: fs.link, Dest: dest,
		MinChannels: 2, MaxChannels: 6, // ChannelsPinned left false: the tuner owns N
		HandshakeTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second,
		DrainTimeout: 30 * time.Second,
	})

	select {
	case err := <-fs.errc:
		t.Fatalf("fake sender error: %v", err)
	default:
	}
	if exit != 0 {
		t.Fatalf("exit = %d (outcome %s), want 0", exit, sum.Outcome)
	}
	if sum.FilesTransferred != 1 {
		t.Fatalf("FilesTransferred = %d, want 1", sum.FilesTransferred)
	}
	got, err := os.ReadFile(filepath.Join(dest, "big.bin"))
	if err != nil || string(got) != string(data) {
		t.Fatalf("big.bin: content mismatch (err %v)", err)
	}

	if n := fs.joinedCount(); n <= 2 {
		t.Fatalf("joined = %d, want > 2 (the adaptive tuner should have opened channels beyond the %d-channel floor)", n, 2)
	}
}
