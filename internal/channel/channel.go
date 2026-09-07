// Package channel is the transport layer of esync (ARCHITECTURE §7): one TCP
// connection per channel, wrapping the encrypted record layer with framed
// wire-message send/recv, control-channel keepalive, and receiver-side
// candidate racing.
package channel

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"esync/internal/crypto/handshake"
	"esync/internal/crypto/record"
	"esync/internal/fault"
	"esync/internal/obs"
	"esync/internal/wire"
)

// Role identifies which end of the session owns a Conn. It selects the record
// key direction: the sender writes s2r and reads r2s, the receiver the reverse.
type Role uint8

const (
	// Sender is the process that owns the source tree.
	Sender Role = iota
	// Receiver is the process that drives the transfer.
	Receiver
)

// ControlChannelID is the fixed id of the control channel.
const ControlChannelID uint8 = 0

// DefaultSocketBuffer is the --socket-buffer default applied to data channels.
const DefaultSocketBuffer = 4 << 20

// Keepalive timings (ARCHITECTURE §7.1). Exposed as vars so tests can shorten
// them; production leaves them at the spec defaults.
var (
	KeepaliveInterval = 15 * time.Second
	KeepaliveDeadline = 45 * time.Second
)

// Conn is one channel: a net.Conn with the record layer installed and framed
// wire-message codecs on top. A Conn has one logical reader (RecvMsg) and any
// number of concurrent writers (SendMsg serialises through the record Writer).
type Conn struct {
	nc   net.Conn
	id   uint8
	role Role

	w *record.Writer
	r *record.Reader

	recvMu sync.Mutex

	lastRecv  atomic.Int64 // unix nanos of the last frame read
	kaOnce    sync.Once
	kaStop    chan struct{}
	kaStopOne sync.Once
	kaExpired atomic.Bool
	pingTok   atomic.Uint64

	kaInterval time.Duration
	kaDeadline time.Duration
}

func newConn(nc net.Conn, id uint8, sess *handshake.Session, role Role) *Conn {
	wdir, rdir := handshake.S2R, handshake.R2S
	if role == Receiver {
		wdir, rdir = handshake.R2S, handshake.S2R
	}
	wEnc, wMac := sess.RecordKeys(wdir, id)
	rEnc, rMac := sess.RecordKeys(rdir, id)
	c := &Conn{
		nc:         nc,
		id:         id,
		role:       role,
		w:          record.NewWriter(nc, wEnc, wMac, id),
		r:          record.NewReader(nc, rEnc, rMac, id),
		kaStop:     make(chan struct{}),
		kaInterval: KeepaliveInterval,
		kaDeadline: KeepaliveDeadline,
	}
	c.lastRecv.Store(time.Now().UnixNano())
	return c
}

// Control wraps a freshly handshaken connection as control channel 0 and tunes
// it for latency (TCP_NODELAY on, default socket buffers).
func Control(nc net.Conn, sess *handshake.Session, role Role) *Conn {
	if t, ok := nc.(*net.TCPConn); ok {
		_ = t.SetNoDelay(true)
	}
	return newConn(nc, ControlChannelID, sess, role)
}

// Data wraps a joined connection as data channel id (1..N) and tunes it for
// throughput (TCP_NODELAY off, enlarged socket buffers).
func Data(nc net.Conn, id uint8, sess *handshake.Session, role Role, socketBuffer int) *Conn {
	if t, ok := nc.(*net.TCPConn); ok {
		_ = t.SetNoDelay(false)
		if socketBuffer > 0 {
			_ = t.SetReadBuffer(socketBuffer)
			_ = t.SetWriteBuffer(socketBuffer)
		}
	}
	return newConn(nc, id, sess, role)
}

// ID returns the channel id.
func (c *Conn) ID() uint8 { return c.id }

// SendMsg marshals m to a frame and writes it as one encrypted record. Safe for
// concurrent use.
func (c *Conn) SendMsg(m wire.Message) error {
	b, err := wire.Marshal(m)
	if err != nil {
		return err
	}
	return c.w.WriteFrame(b)
}

// RecvMsg reads the next wire message. A clean peer close returns io.EOF.
// PING/PONG traffic is handled transparently and never returned to the caller.
// A keepalive-deadline breach returns E3004.
func (c *Conn) RecvMsg() (wire.Message, error) {
	c.recvMu.Lock()
	defer c.recvMu.Unlock()
	for {
		frame, err := c.r.ReadFrame()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			if c.kaExpired.Load() {
				return nil, fault.New(fault.E3004, "keepalive", "control channel", nil)
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return nil, fault.Wrap(fault.E3004, "recv message", "", err)
			}
			return nil, err
		}
		c.lastRecv.Store(time.Now().UnixNano())
		msg, err := wire.Unmarshal(frame)
		if err != nil {
			return nil, err
		}
		switch v := msg.(type) {
		case *wire.Ping:
			if err := c.SendMsg(&wire.Pong{Token: v.Token}); err != nil {
				return nil, err
			}
			continue
		case *wire.Pong:
			continue
		}
		return msg, nil
	}
}

// RecvMsgTimeout is RecvMsg with a hard ceiling on how long a single receive may
// block. On expiry it returns E3005 and the stream position is indeterminate, so
// the caller must treat the Conn as dead. It is meant for data channels, which
// carry no keepalive; do not use it on the control channel (whose RecvMsg the
// keepalive watchdog already bounds).
func (c *Conn) RecvMsgTimeout(d time.Duration) (wire.Message, error) {
	if d <= 0 {
		return c.RecvMsg()
	}
	_ = c.nc.SetReadDeadline(time.Now().Add(d))
	defer func() { _ = c.nc.SetReadDeadline(time.Time{}) }()
	m, err := c.RecvMsg()
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return nil, fault.New(fault.E3005, "data channel idle past the stall timeout", "", err)
		}
	}
	return m, err
}

// SetKeepaliveTimings overrides the ping interval and dead-peer deadline for
// this Conn. Call before EnableKeepalive.
func (c *Conn) SetKeepaliveTimings(interval, deadline time.Duration) {
	c.kaInterval, c.kaDeadline = interval, deadline
}

// EnableKeepalive starts the control-channel liveness machinery: a PING every
// idle interval and a watchdog that trips E3004 after the deadline of silence.
// Idempotent. Requires RecvMsg to be pumped by some goroutine.
func (c *Conn) EnableKeepalive(ctx obs.Ctx) {
	c.kaOnce.Do(func() {
		obs.Go(ctx, "keepalive.ping", func() error { c.pingLoop(); return nil })
		obs.Go(ctx, "keepalive.watch", func() error { c.watchLoop(); return nil })
	})
}

func (c *Conn) pingLoop() {
	t := time.NewTicker(c.kaInterval)
	defer t.Stop()
	for {
		select {
		case <-c.kaStop:
			return
		case <-t.C:
			if time.Since(time.Unix(0, c.lastRecv.Load())) < c.kaInterval {
				continue
			}
			if err := c.SendMsg(&wire.Ping{Token: c.pingTok.Add(1)}); err != nil {
				return
			}
		}
	}
}

func (c *Conn) watchLoop() {
	t := time.NewTicker(c.kaInterval / 3)
	defer t.Stop()
	for {
		select {
		case <-c.kaStop:
			return
		case <-t.C:
			if time.Since(time.Unix(0, c.lastRecv.Load())) > c.kaDeadline {
				c.kaExpired.Store(true)
				_ = c.nc.SetReadDeadline(time.Now())
				return
			}
		}
	}
}

// Close writes the authenticated CLOSE record and closes the socket. Idempotent.
func (c *Conn) Close() error {
	c.kaStopOne.Do(func() { close(c.kaStop) })
	werr := c.w.Close()
	cerr := c.nc.Close()
	if werr != nil {
		return werr
	}
	return cerr
}

// NetConn exposes the underlying connection for deadline management during the
// pairing phase.
func (c *Conn) NetConn() net.Conn { return c.nc }
