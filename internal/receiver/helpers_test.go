package receiver

import (
	"bytes"
	"net"
	"testing"
	"time"

	"esync/internal/channel"
	"esync/internal/crypto/handshake"
	"esync/internal/obs"
)

var testKPair = obs.NewSecret(bytes.Repeat([]byte{0x42}, 32))
var testSID = [8]byte{9, 7, 5, 3, 1, 2, 4, 6}

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

// connPair returns a sender-side and receiver-side channel.Conn for one channel
// id, over loopback TCP with a real handshake.
func connPair(t *testing.T, id uint8) (senderSide, receiverSide *channel.Conn) {
	t.Helper()
	cli, srv := tcpPair(t)
	type r struct {
		s   *handshake.Session
		err error
	}
	cch := make(chan r, 1)
	go func() {
		s, err := handshake.ClientHandshake(obs.Ctx{}, cli, testKPair, testSID, 3*time.Second)
		cch <- r{s, err}
	}()
	ssrv, err := handshake.ServerHandshake(obs.Ctx{}, srv, testKPair, testSID, 3*time.Second)
	if err != nil {
		t.Fatalf("server handshake: %v", err)
	}
	cr := <-cch
	if cr.err != nil {
		t.Fatalf("client handshake: %v", cr.err)
	}
	if id == channel.ControlChannelID {
		return channel.Control(srv, ssrv, channel.Sender), channel.Control(cli, cr.s, channel.Receiver)
	}
	return channel.Data(srv, id, ssrv, channel.Sender, 0), channel.Data(cli, id, cr.s, channel.Receiver, 0)
}
