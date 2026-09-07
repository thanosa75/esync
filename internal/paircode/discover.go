package paircode

import (
	"net"
	"net/netip"
	"sort"
	"strings"

	"esync/internal/fault"
)

// Discover enumerates the local addresses to advertise in a pairing code (§5.4).
//
// It keeps interfaces that are UP, not loopback, and have at least one
// non-link-local address, then ranks their global addresses: global IPv4 on a
// wired interface > global IPv4 on wireless > ULA/global IPv6 > link-local IPv6
// with zone. The top min(4, n) are returned, Port left zero for the caller to
// fill once the listener is bound.
//
// If bind is non-empty it is parsed as a single IP literal and returned as the
// only endpoint (the caller then sets Payload.Restricted). Zero usable
// addresses is E1005.
//
// Wired-vs-wireless is a best-effort guess from the interface name; an unknown
// name is treated as wired. This is a documented limitation (§5.4).
func Discover(bind string) ([]Endpoint, error) {
	if bind != "" {
		addr, err := netip.ParseAddr(strings.TrimSpace(bind))
		if err != nil || addr.IsUnspecified() {
			return nil, fault.Newf(fault.E1005, "resolve --bind", bind, err, "not an IP address")
		}
		return []Endpoint{{Addr: addr}}, nil
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, fault.New(fault.E1005, "enumerate interfaces", "", err)
	}

	type ranked struct {
		ep    Endpoint
		score int
	}
	var cands []ranked

	for _, ifi := range ifaces {
		if ifi.Flags&net.FlagUp == 0 || ifi.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := ifi.Addrs()
		if err != nil {
			continue
		}
		var parsed []netip.Addr
		hasRoutable := false
		for _, a := range addrs {
			p, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip, ok := netip.AddrFromSlice(p.IP)
			if !ok {
				continue
			}
			ip = ip.Unmap()
			parsed = append(parsed, ip)
			if !ip.IsLinkLocalUnicast() && !ip.IsLoopback() {
				hasRoutable = true
			}
		}
		if !hasRoutable {
			continue
		}
		wired := isWired(ifi.Name)
		for _, ip := range parsed {
			s := score(ip, wired)
			if s == 0 {
				continue
			}
			ep := Endpoint{Addr: ip}
			if ip.IsLinkLocalUnicast() {
				ep.Addr = ip.WithZone(ifi.Name)
			}
			cands = append(cands, ranked{ep: ep, score: s})
		}
	}

	if len(cands) == 0 {
		return nil, fault.New(fault.E1005, "discover endpoints", "", nil)
	}

	sort.SliceStable(cands, func(i, j int) bool { return cands[i].score > cands[j].score })

	n := len(cands)
	if n > maxEndpoints {
		n = maxEndpoints
	}
	out := make([]Endpoint, 0, n)
	for _, c := range cands[:n] {
		out = append(out, c.ep)
	}
	return out, nil
}

// score ranks an address; 0 means "do not advertise".
func score(ip netip.Addr, wired bool) int {
	switch {
	case ip.Is4() && ip.IsGlobalUnicast() && !ip.IsPrivate():
		if wired {
			return 100
		}
		return 90
	case ip.Is4() && ip.IsPrivate():
		if wired {
			return 80
		}
		return 70
	case ip.Is6() && (ip.IsGlobalUnicast() || ip.IsPrivate()) && !ip.IsLinkLocalUnicast():
		return 50
	case ip.Is6() && ip.IsLinkLocalUnicast():
		return 20
	default:
		return 0
	}
}

func isWired(name string) bool {
	n := strings.ToLower(name)
	for _, p := range []string{"wl", "wlan", "wifi", "wlp", "ath", "ra"} {
		if strings.HasPrefix(n, p) {
			return false
		}
	}
	return true
}
