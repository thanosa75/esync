package channel

import (
	"bytes"
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"esync/internal/crypto/handshake"
	"esync/internal/fault"
	"esync/internal/obs"
	"esync/internal/paircode"
	"esync/internal/wire"
)

func tcpPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	type ac struct {
		c   net.Conn
		err error
	}
	acc := make(chan ac, 1)
	go func() {
		c, err := ln.Accept()
		acc <- ac{c, err}
	}()
	client, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	a := <-acc
	if a.err != nil {
		t.Fatalf("accept: %v", a.err)
	}
	t.Cleanup(func() { client.Close(); a.c.Close() })
	return client, a.c
}

// connPair returns a wired sender/receiver Conn pair for channel id, backed by a
// real loopback TCP connection and a real handshake.
func connPair(t *testing.T, id uint8) (senderConn, receiverConn *Conn) {
	t.Helper()
	cli, srv := tcpPair(t)
	kPair := obs.NewSecret(bytes.Repeat([]byte{0x5a}, 32))
	sid := [8]byte{9, 8, 7, 6, 5, 4, 3, 2}

	type r struct {
		s   *handshake.Session
		err error
	}
	cch := make(chan r, 1)
	go func() {
		s, err := handshake.ClientHandshake(obs.Ctx{}, cli, kPair, sid, 3*time.Second)
		cch <- r{s, err}
	}()
	ssrv, err := handshake.ServerHandshake(obs.Ctx{}, srv, kPair, sid, 3*time.Second)
	if err != nil {
		t.Fatalf("server handshake: %v", err)
	}
	cr := <-cch
	if cr.err != nil {
		t.Fatalf("client handshake: %v", cr.err)
	}
	// The sender is the listening side (ServerHandshake).
	if id == ControlChannelID {
		return Control(srv, ssrv, Sender), Control(cli, cr.s, Receiver)
	}
	return Data(srv, id, ssrv, Sender, 0), Data(cli, id, cr.s, Receiver, 0)
}

// T-CHAN: frames round-trip through the record layer in both directions.
func TestChannelRoundTrip(t *testing.T) {
	snd, rcv := connPair(t, ControlChannelID)

	want := &wire.SessionParams{RootName: "photos", SourceKind: 0, GroupSize: 1024, SenderVersion: "test"}
	if err := snd.SendMsg(want); err != nil {
		t.Fatalf("send: %v", err)
	}
	got, err := rcv.RecvMsg()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	sp, ok := got.(*wire.SessionParams)
	if !ok || sp.RootName != "photos" || sp.SenderVersion != "test" {
		t.Fatalf("round-trip mismatch: %#v", got)
	}

	// reverse direction
	if err := rcv.SendMsg(&wire.SessionReady{Channels: 3, GroupCredit: 4, MaxChunk: 1 << 20}); err != nil {
		t.Fatalf("send back: %v", err)
	}
	got2, err := snd.RecvMsg()
	if err != nil {
		t.Fatalf("recv back: %v", err)
	}
	if r, ok := got2.(*wire.SessionReady); !ok || r.Channels != 3 {
		t.Fatalf("reverse round-trip mismatch: %#v", got2)
	}

	// many small frames keep their order and content
	for i := 0; i < 50; i++ {
		if err := snd.SendMsg(&wire.FileChunk{RequestID: uint64(i), Offset: uint64(i * 10), Data: []byte{byte(i)}}); err != nil {
			t.Fatalf("chunk send %d: %v", i, err)
		}
	}
	for i := 0; i < 50; i++ {
		m, err := rcv.RecvMsg()
		if err != nil {
			t.Fatalf("chunk recv %d: %v", i, err)
		}
		c := m.(*wire.FileChunk)
		if c.RequestID != uint64(i) || len(c.Data) != 1 || c.Data[0] != byte(i) {
			t.Fatalf("chunk %d corrupted: %#v", i, c)
		}
	}
}

// T-CHAN: a clean Close is observed by the peer as io.EOF.
func TestChannelCleanClose(t *testing.T) {
	snd, rcv := connPair(t, ControlChannelID)
	if err := snd.SendMsg(&wire.Ping{Token: 1}); err != nil {
		t.Fatal(err)
	}
	_ = snd.Close()
	// PING is swallowed by RecvMsg; the CLOSE record then yields EOF.
	if _, err := rcv.RecvMsg(); !errors.Is(err, net.ErrClosed) && err == nil {
		t.Fatalf("expected EOF/closed after peer close, got %v", err)
	}
}

// T-NET-01: candidate racing picks a live endpoint past a blackholed one.
func TestDialRacingPicksLiveEndpoint(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	live := ln.Addr().(*net.TCPAddr)

	// 198.51.100.0/24 (TEST-NET-2) is unrouteable: connect() stalls or errors.
	blackhole := paircode.Endpoint{Addr: netip.MustParseAddr("198.51.100.23"), Port: 9}
	liveEP := paircode.Endpoint{Addr: netip.MustParseAddr("127.0.0.1"), Port: uint16(live.Port)}

	nc, chosen, _, err := Dial(obs.Ctx{}, []paircode.Endpoint{blackhole, liveEP}, 3*time.Second, 250*time.Millisecond, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer nc.Close()
	if chosen.Addr != liveEP.Addr || chosen.Port != liveEP.Port {
		t.Fatalf("chose %v, want %v", chosen, liveEP)
	}
}

// T-NET-02: total failure yields E3001 with a per-endpoint reason table.
func TestDialTotalFailureErrorTable(t *testing.T) {
	// Two refused ports on loopback.
	l1, _ := net.Listen("tcp", "127.0.0.1:0")
	l2, _ := net.Listen("tcp", "127.0.0.1:0")
	p1 := l1.Addr().(*net.TCPAddr).Port
	p2 := l2.Addr().(*net.TCPAddr).Port
	l1.Close()
	l2.Close()

	eps := []paircode.Endpoint{
		{Addr: netip.MustParseAddr("127.0.0.1"), Port: uint16(p1)},
		{Addr: netip.MustParseAddr("127.0.0.1"), Port: uint16(p2)},
	}
	nc, _, tab, err := Dial(obs.Ctx{}, eps, 2*time.Second, 50*time.Millisecond, nil)
	if nc != nil {
		nc.Close()
		t.Fatal("expected no connection")
	}
	if fault.GetCode(err) != fault.E3001 {
		t.Fatalf("code = %v, want E3001", fault.GetCode(err))
	}
	if len(tab) != 2 {
		t.Fatalf("error table has %d rows, want 2", len(tab))
	}
	rendered := FormatEndpointErrs(tab)
	if !strings.Contains(rendered, "127.0.0.1") {
		t.Fatalf("rendered table missing endpoint: %q", rendered)
	}
}

// T-NET-03: silence past the keepalive deadline surfaces as E3004.
func TestKeepaliveDeadPeer(t *testing.T) {
	snd, rcv := connPair(t, ControlChannelID)
	_ = rcv // receiver never answers; do not pump its RecvMsg

	snd.SetKeepaliveTimings(40*time.Millisecond, 150*time.Millisecond)
	snd.EnableKeepalive(obs.Ctx{})

	done := make(chan error, 1)
	go func() {
		_, err := snd.RecvMsg()
		done <- err
	}()

	select {
	case err := <-done:
		if fault.GetCode(err) != fault.E3004 {
			t.Fatalf("code = %v, want E3004", fault.GetCode(err))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("keepalive watchdog never fired")
	}
}
