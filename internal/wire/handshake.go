package wire

import "esync/internal/fault"

// The cleartext handshake and channel-auth wire forms (§6.2, §6.4). These are
// NOT frames and NOT records: they are sent on the raw connection before any key
// is installed. crypto/handshake and channel encode/decode them through here.

// Fixed sizes of the handshake messages.
const (
	ClientHelloLen   = len(Magic) + 1 + 8 + 32 + 32 // 78
	ServerHelloLen   = 1 + 32 + 32                  // 65
	ConfirmLen       = 32
	ChannelJoinLen   = len(Magic) + 1 + 8 + 1 + 16 + 32 // 63
	ChannelAcceptLen = 32
)

func badHandshake(op string) error {
	return fault.New(fault.E5003, op, "", nil)
}

// ClientHello: magic "ESYNC" | ver:u8 | session_id:8 | nonce_c:32 | Pc:32.
type ClientHello struct {
	Version   uint8
	SessionID [8]byte
	NonceC    [32]byte
	Pc        [32]byte
}

func (h *ClientHello) Encode() []byte {
	w := &writer{}
	w.raw([]byte(Magic))
	w.u8(h.Version)
	w.raw(h.SessionID[:])
	w.raw(h.NonceC[:])
	w.raw(h.Pc[:])
	return w.b
}

func DecodeClientHello(b []byte) (*ClientHello, error) {
	if len(b) != ClientHelloLen || string(b[:len(Magic)]) != Magic {
		return nil, badHandshake("decode CLIENT_HELLO")
	}
	p := len(Magic)
	h := &ClientHello{Version: b[p]}
	p++
	p += copy(h.SessionID[:], b[p:p+8])
	p += copy(h.NonceC[:], b[p:p+32])
	copy(h.Pc[:], b[p:p+32])
	return h, nil
}

// ServerHello: ver:u8 | nonce_s:32 | Ps:32.
type ServerHello struct {
	Version uint8
	NonceS  [32]byte
	Ps      [32]byte
}

func (h *ServerHello) Encode() []byte {
	w := &writer{}
	w.u8(h.Version)
	w.raw(h.NonceS[:])
	w.raw(h.Ps[:])
	return w.b
}

func DecodeServerHello(b []byte) (*ServerHello, error) {
	if len(b) != ServerHelloLen {
		return nil, badHandshake("decode SERVER_HELLO")
	}
	h := &ServerHello{Version: b[0]}
	copy(h.NonceS[:], b[1:33])
	copy(h.Ps[:], b[33:65])
	return h, nil
}

// ServerConfirm: tag:32 = HMAC-SHA256(K_conf, "esync/v1 server confirm" || T).
type ServerConfirm struct{ Tag [32]byte }

func (c *ServerConfirm) Encode() []byte { return append([]byte(nil), c.Tag[:]...) }

func DecodeServerConfirm(b []byte) (*ServerConfirm, error) {
	if len(b) != ConfirmLen {
		return nil, badHandshake("decode SERVER_CONFIRM")
	}
	c := &ServerConfirm{}
	copy(c.Tag[:], b)
	return c, nil
}

// ClientConfirm: tag:32 = HMAC-SHA256(K_conf, "esync/v1 client confirm" || T).
type ClientConfirm struct{ Tag [32]byte }

func (c *ClientConfirm) Encode() []byte { return append([]byte(nil), c.Tag[:]...) }

func DecodeClientConfirm(b []byte) (*ClientConfirm, error) {
	if len(b) != ConfirmLen {
		return nil, badHandshake("decode CLIENT_CONFIRM")
	}
	c := &ClientConfirm{}
	copy(c.Tag[:], b)
	return c, nil
}

// ChannelJoin: magic | ver:u8 | session_id:8 | channel_id:u8 | nonce_ch:16 | tag:32.
type ChannelJoin struct {
	Version   uint8
	SessionID [8]byte
	ChannelID uint8
	NonceCh   [16]byte
	Tag       [32]byte
}

func (j *ChannelJoin) Encode() []byte {
	w := &writer{}
	w.raw([]byte(Magic))
	w.u8(j.Version)
	w.raw(j.SessionID[:])
	w.u8(j.ChannelID)
	w.raw(j.NonceCh[:])
	w.raw(j.Tag[:])
	return w.b
}

func DecodeChannelJoin(b []byte) (*ChannelJoin, error) {
	if len(b) != ChannelJoinLen || string(b[:len(Magic)]) != Magic {
		return nil, badHandshake("decode CHANNEL_JOIN")
	}
	p := len(Magic)
	j := &ChannelJoin{Version: b[p]}
	p++
	p += copy(j.SessionID[:], b[p:p+8])
	j.ChannelID = b[p]
	p++
	p += copy(j.NonceCh[:], b[p:p+16])
	copy(j.Tag[:], b[p:p+32])
	return j, nil
}

// ChannelAccept: tag:32 = HMAC(K_chan, "accept" || session_id || channel_id || nonce_ch).
type ChannelAccept struct{ Tag [32]byte }

func (a *ChannelAccept) Encode() []byte { return append([]byte(nil), a.Tag[:]...) }

func DecodeChannelAccept(b []byte) (*ChannelAccept, error) {
	if len(b) != ChannelAcceptLen {
		return nil, badHandshake("decode CHANNEL_ACCEPT")
	}
	a := &ChannelAccept{}
	copy(a.Tag[:], b)
	return a, nil
}
