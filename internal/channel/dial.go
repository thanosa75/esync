package channel

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"time"

	"esync/internal/fault"
	"esync/internal/obs"
	"esync/internal/paircode"
)

// EndpointErr records one candidate endpoint's outcome during a failed Dial.
type EndpointErr struct {
	Endpoint paircode.Endpoint
	Err      error
	Elapsed  time.Duration
}

// HandshakeFunc performs the encrypted handshake on a freshly connected socket.
// It returns nil when the peer is the genuine sender for this pairing code.
type HandshakeFunc func(ctx context.Context, nc net.Conn) error

// Dial races all candidate endpoints (ARCHITECTURE §7.2): each start is
// staggered by stagger, every attempt runs under connectTimeout, and the first
// candidate to complete the handshake wins. Losers are cancelled and closed.
// On total failure it returns E3001 carrying the per-endpoint reason table.
func Dial(ctx obs.Ctx, endpoints []paircode.Endpoint, connectTimeout, stagger time.Duration, hs HandshakeFunc) (net.Conn, paircode.Endpoint, []EndpointErr, error) {
	if len(endpoints) == 0 {
		return nil, paircode.Endpoint{}, nil, fault.New(fault.E3001, "dial sender", "", nil)
	}

	dctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	type outcome struct {
		ep    paircode.Endpoint
		nc    net.Conn
		err   error
		dur   time.Duration
		wonHS bool
	}
	results := make(chan outcome, len(endpoints))

	for i, ep := range endpoints {
		i, ep := i, ep
		obs.Go(ctx, "dial."+addrOf(ep), func() error {
			select {
			case <-time.After(time.Duration(i) * stagger):
			case <-dctx.Done():
				results <- outcome{ep: ep, err: context.Cause(dctx)}
				return nil
			}
			start := time.Now()
			var d net.Dialer
			nc, err := d.DialContext(dctx, "tcp", addrOf(ep))
			if err != nil {
				results <- outcome{ep: ep, err: err, dur: time.Since(start)}
				return nil
			}
			if hs != nil {
				if err := hs(dctx, nc); err != nil {
					_ = nc.Close()
					results <- outcome{ep: ep, err: err, dur: time.Since(start)}
					return nil
				}
			}
			results <- outcome{ep: ep, nc: nc, dur: time.Since(start), wonHS: true}
			return nil
		})
	}

	var errs []EndpointErr
	for i := 0; i < len(endpoints); i++ {
		r := <-results
		if r.wonHS {
			cancel()
			obs.Go(ctx, "dial.drain", func() error {
				for j := i + 1; j < len(endpoints); j++ {
					if rr := <-results; rr.nc != nil {
						_ = rr.nc.Close()
					}
				}
				return nil
			})
			return r.nc, r.ep, errs, nil
		}
		errs = append(errs, EndpointErr{Endpoint: r.ep, Err: r.err, Elapsed: r.dur})
	}
	return nil, paircode.Endpoint{}, errs, fault.Newf(fault.E3001, "dial sender", "", nil,
		"no candidate endpoint reachable:\n%s", FormatEndpointErrs(errs))
}

func addrOf(ep paircode.Endpoint) string {
	return netip.AddrPortFrom(ep.Addr, ep.Port).String()
}

// FormatEndpointErrs renders the per-endpoint reason table of §7.2.
func FormatEndpointErrs(errs []EndpointErr) string {
	var b strings.Builder
	for _, e := range errs {
		reason := "unknown error"
		if e.Err != nil {
			reason = e.Err.Error()
		}
		fmt.Fprintf(&b, "  %-21s %-28s (after %s)\n",
			addrOf(e.Endpoint), reason, e.Elapsed.Round(time.Millisecond))
	}
	b.WriteString("  hint: are both machines on the same network? is a firewall blocking inbound TCP on the sender?")
	return b.String()
}
