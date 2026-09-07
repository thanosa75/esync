package channel

import (
	"net"
	"net/netip"
	"strings"
	"time"

	"esync/internal/fault"
)

// Listener is the sender's inbound socket: it accepts the control connection
// during pairing and the data-channel connections afterward.
type Listener struct {
	ln *net.TCPListener
}

// Listen binds a TCP listener. An empty bind listens on all interfaces; a
// non-empty bind must be a literal IP address. port 0 picks a free port.
func Listen(bind string, port int) (*Listener, error) {
	addr := &net.TCPAddr{Port: port}
	if b := strings.TrimSpace(bind); b != "" {
		ip, err := netip.ParseAddr(b)
		if err != nil {
			return nil, fault.Newf(fault.E1005, "parse bind address", bind, err, "not an IP address")
		}
		addr.IP = net.IP(ip.AsSlice())
	}
	ln, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, fault.Wrap(fault.E3001, "bind listener", addr.String(), err)
	}
	return &Listener{ln: ln}, nil
}

// Port returns the bound TCP port.
func (l *Listener) Port() uint16 {
	return uint16(l.ln.Addr().(*net.TCPAddr).Port)
}

// Accept returns the next raw connection. The caller performs the handshake.
func (l *Listener) Accept() (net.Conn, error) {
	nc, err := l.ln.Accept()
	if err != nil {
		return nil, fault.Wrap(fault.E3001, "accept connection", "", err)
	}
	return nc, nil
}

// SetDeadline bounds Accept; a zero time clears it.
func (l *Listener) SetDeadline(t time.Time) error { return l.ln.SetDeadline(t) }

// Close stops listening.
func (l *Listener) Close() error { return l.ln.Close() }
