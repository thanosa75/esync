package wire

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"testing"

	"esync/internal/fault"
)

// sampleMessages is the fixed set exercised by the golden and round-trip tests.
// One representative value per §9.3 body, with deterministic field contents.
func sampleMessages() []Message {
	return []Message{
		&SessionParams{
			RootName: "Documents", SourceKind: 0, HashAlg: 0, GroupBytes: 512 << 20,
			Flags: 0b10001, SourcePlatform: 0, PathNorm: 1, SenderVersion: "esync/0.1",
			MaxChannels: 16, FilterSignature: "", SysexcludeTag: "sys-v1",
		},
		&SessionReady{
			Channels: 4, GroupCredit: 8, PipelineDepth: 2, MaxChunk: 1 << 20,
			DestPlatform: 1, ReceiverVersion: "esync/0.1", DestFreeBytes: 1 << 40,
		},
		&ScanProgress{EntriesSeen: 1234, BytesSeen: 5678901, ErrorsSeen: 2},
		&ScanComplete{
			TotalFiles: 100, TotalBytes: 1 << 30, TotalGroups: 1, SkippedEntries: 3,
			ManifestDigest: bytes.Repeat([]byte{0xab}, 32),
		},
		&GroupManifest{
			GroupID: 0, FirstFileID: 0,
			Entries: []ManifestEntry{
				{
					EntryType: 0, Flags: ManifestFlagHasHardlinkKey | 0b1000,
					Path: []byte("a/b.txt"), Size: 42, Mode: 0o644,
					MtimeSec: 1_700_000_000, MtimeNsec: 123, Digest: bytes.Repeat([]byte{0x11}, 16),
					HardlinkKey: 0x99,
				},
				{
					EntryType: 1, Flags: 0, Path: []byte("a"), Size: 0, Mode: 0o755,
					MtimeSec: 1_700_000_001, MtimeNsec: 0,
				},
				{
					EntryType: 2, Flags: 0, Path: []byte("a/link"), Size: 0, Mode: 0o777,
					MtimeSec: 1_700_000_002, MtimeNsec: 0, LinkTarget: []byte("b.txt"),
				},
			},
		},
		&GroupDecision{
			GroupID: 0, Needed: []uint16{0, 2, 5}, SkippedCount: 1,
			Rejected: []RejectedEntry{{Index: 3, ErrorCode: 7010}},
		},
		&Credit{AdditionalGroups: 12},
		&FileRequest{RequestID: 1, FileID: 2, Offset: 0, MaxChunk: 1 << 20},
		&FileHeader{
			RequestID: 1, FileID: 2, Size: 42, Mode: 0o644, MtimeSec: 1_700_000_000,
			MtimeNsec: 123, Digest: bytes.Repeat([]byte{0x11}, 16), ChunkSize: 1 << 20,
		},
		&FileChunk{RequestID: 1, Offset: 0, Data: []byte("hello world")},
		&FileComplete{RequestID: 1, BytesSent: 11, DigestFull: bytes.Repeat([]byte{0x22}, 16)},
		&FileError{RequestID: 1, Code: 6001, Retryable: 0, Message: "permission denied"},
		&FileCancel{RequestID: 1, Reason: 2},
		&SessionSummary{
			FilesTotal: 100, FilesTransferred: 97, FilesSkipped: 2, FilesFailed: 1,
			BytesTransferred: 1 << 30, GroupsTotal: 1, CompletionDigest: bytes.Repeat([]byte{0xcd}, 32),
		},
		&SessionSummaryAck{
			FilesTotal: 100, FilesTransferred: 97, FilesSkipped: 2, FilesFailed: 1,
			BytesTransferred: 1 << 30, GroupsTotal: 1, CompletionDigest: bytes.Repeat([]byte{0xcd}, 32),
		},
		&Error{Code: 5001, Fatal: 1, Message: "bad state", Detail: "SESSION_PARAMS in Serving"},
		&Ping{Token: 0xdeadbeef},
		&Pong{Token: 0xdeadbeef},
		&SessionResume{
			SessionID: 0x0102030405060708, Nonce: bytes.Repeat([]byte{0x33}, 16),
			Tag: bytes.Repeat([]byte{0x44}, 32), LastSeqSeen: 987,
		},
	}
}

// G-WIRE-01: each message type marshals to exactly these frame bytes
// (msg_type u8 | body_len u32 BE | body). Regenerate only on a deliberate
// wire-format change.
var goldenWire = map[MsgType]string{
	MsgSessionParams:     "01000000340009446f63756d656e74730000000000002000000000000011000100096573796e632f302e3100000010000000067379732d7631",
	MsgSessionReady:      "020000001d0004000802001000000100096573796e632f302e310000010000000000",
	MsgScanProgress:      "100000001400000000000004d2000000000056a73500000002",
	MsgScanComplete:      "110000003a0000000000000064000000004000000000000001000000030020abababababababababababababababababababababababababababababababab",
	MsgGroupManifest:     "120000009500000000000000000000000000030000090007612f622e747874000000000000002a000001a4000000006553f1000000007b101111111111111111111111111111111100000000000000990100000001610000000000000000000001ed000000006553f10100000000000200000006612f6c696e6b0000000000000000000001ff000000006553f10200000000000005622e747874",
	MsgGroupDecision:     "20000000140000000000030000000200050001000100031b62",
	MsgCredit:            "2100000002000c",
	MsgFileRequest:       "300000001c00000000000000010000000000000002000000000000000000100000",
	MsgFileHeader:        "310000003d00000000000000010000000000000002000000000000002a000001a4000000006553f1000000007b101111111111111111111111111111111100100000",
	MsgFileChunk:         "320000001f000000000000000100000000000000000000000b68656c6c6f20776f726c64",
	MsgFileComplete:      "33000000210000000000000001000000000000000b1022222222222222222222222222222222",
	MsgFileError:         "340000001e000000000000000117710000117065726d697373696f6e2064656e696564",
	MsgFileCancel:        "350000000a00000000000000010002",
	MsgSessionSummary:    "400000004e00000000000000640000000000000061000000000000000200000000000000010000000040000000000000010020cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
	MsgSessionSummaryAck: "410000004e00000000000000640000000000000061000000000000000200000000000000010000000040000000000000010020cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
	MsgError:             "50000000291389010009626164207374617465001953455353494f4e5f504152414d5320696e2053657276696e67",
	MsgPing:              "600000000800000000deadbeef",
	MsgPong:              "610000000800000000deadbeef",
	MsgSessionResume:     "700000004401020304050607080010333333333333333333333333333333330020444444444444444444444444444444444444444444444444444444444444444400000000000003db",
}

func TestGoldenWire(t *testing.T) {
	for _, m := range sampleMessages() {
		frame, err := Marshal(m)
		if err != nil {
			t.Fatalf("%s: marshal: %v", m.Type(), err)
		}
		got := hex.EncodeToString(frame)
		want := normHex(goldenWire[m.Type()])
		if want == "" {
			t.Errorf("%s: no golden vector; actual = %s", m.Type(), got)
			continue
		}
		if got != want {
			t.Errorf("%s:\n got  %s\n want %s", m.Type(), got, want)
		}
	}
}

func normHex(s string) string {
	var b []byte
	for i := 0; i < len(s); i++ {
		if s[i] != ' ' {
			b = append(b, s[i])
		}
	}
	return string(b)
}

// T-PROTO round-trip: Unmarshal(Marshal(m)) deep-equals m for every body.
func TestFrameRoundTrip(t *testing.T) {
	for _, m := range sampleMessages() {
		frame, err := Marshal(m)
		if err != nil {
			t.Fatalf("%s: marshal: %v", m.Type(), err)
		}
		got, err := Unmarshal(frame)
		if err != nil {
			t.Fatalf("%s: unmarshal: %v", m.Type(), err)
		}
		if !reflect.DeepEqual(got, m) {
			t.Errorf("%s round-trip mismatch:\n got  %#v\n want %#v", m.Type(), got, m)
		}
	}
}

// Error.Detail is an opt trailing field: absent when body_len ends before it.
func TestErrorOptDetail(t *testing.T) {
	frame, _ := Marshal(&Error{Code: 1, Fatal: 0, Message: "x", Detail: ""})
	got, err := Unmarshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	if e := got.(*Error); e.Detail != "" {
		t.Errorf("Detail = %q, want empty", e.Detail)
	}
	// Truncate the frame to drop the detail str entirely.
	short := frame[:len(frame)-2]
	short[4] = byte(len(short) - FrameHeaderLen) // fix body_len (value fits one byte)
	if _, err := Unmarshal(short); err != nil {
		t.Errorf("unmarshal without opt detail: %v", err)
	}
}

func TestUnmarshalErrors(t *testing.T) {
	// Unknown msg_type → E5011.
	if _, err := Unmarshal([]byte{0xff, 0, 0, 0, 0}); fault.GetCode(err) != fault.E5011 {
		t.Errorf("unknown type: code = %v, want E5011", fault.GetCode(err))
	}
	// body_len disagrees with plaintext → E5010.
	if _, err := Unmarshal([]byte{byte(MsgPing), 0, 0, 0, 99}); fault.GetCode(err) != fault.E5010 {
		t.Errorf("body_len mismatch: code = %v, want E5010", fault.GetCode(err))
	}
	// Truncated body → E5003.
	if _, err := Unmarshal([]byte{byte(MsgPing), 0, 0, 0, 3, 1, 2, 3}); fault.GetCode(err) != fault.E5003 {
		t.Errorf("short body: code = %v, want E5003", fault.GetCode(err))
	}
	// Too short for a header.
	if _, err := Unmarshal([]byte{0x01}); fault.GetCode(err) != fault.E5010 {
		t.Errorf("short frame: code = %v, want E5010", fault.GetCode(err))
	}
}

// REQ-SEC-014: an oversized str length is rejected before allocation.
func TestBoundsBeforeAlloc(t *testing.T) {
	// SESSION_PARAMS body claiming a 0xffff-byte root_name in a 7-byte body.
	body := []byte{0xff, 0xff, 'a', 'b', 'c', 'd', 'e'}
	frame := append([]byte{byte(MsgSessionParams), 0, 0, 0, byte(len(body))}, body...)
	if _, err := Unmarshal(frame); fault.GetCode(err) != fault.E5003 {
		t.Errorf("oversized str: code = %v, want E5003", fault.GetCode(err))
	}
}

func TestHandshakeRoundTrip(t *testing.T) {
	ch := &ClientHello{Version: 1}
	copy(ch.SessionID[:], []byte("SESSION1"))
	for i := range ch.NonceC {
		ch.NonceC[i] = byte(i)
	}
	for i := range ch.Pc {
		ch.Pc[i] = byte(0x40 + i)
	}
	if got, err := DecodeClientHello(ch.Encode()); err != nil || !reflect.DeepEqual(got, ch) {
		t.Errorf("ClientHello: err=%v got=%#v", err, got)
	}
	if len(ch.Encode()) != ClientHelloLen {
		t.Errorf("ClientHello len = %d, want %d", len(ch.Encode()), ClientHelloLen)
	}

	sh := &ServerHello{Version: 1}
	sh.NonceS[0], sh.Ps[0] = 9, 8
	if got, err := DecodeServerHello(sh.Encode()); err != nil || !reflect.DeepEqual(got, sh) {
		t.Errorf("ServerHello: err=%v got=%#v", err, got)
	}

	sc := &ServerConfirm{}
	sc.Tag[3] = 0x7
	if got, err := DecodeServerConfirm(sc.Encode()); err != nil || !reflect.DeepEqual(got, sc) {
		t.Errorf("ServerConfirm: err=%v got=%#v", err, got)
	}
	cc := &ClientConfirm{}
	cc.Tag[3] = 0x7
	if got, err := DecodeClientConfirm(cc.Encode()); err != nil || !reflect.DeepEqual(got, cc) {
		t.Errorf("ClientConfirm: err=%v got=%#v", err, got)
	}

	j := &ChannelJoin{Version: 1, ChannelID: 3}
	copy(j.SessionID[:], []byte("SESSION1"))
	j.NonceCh[0] = 1
	j.Tag[0] = 2
	if got, err := DecodeChannelJoin(j.Encode()); err != nil || !reflect.DeepEqual(got, j) {
		t.Errorf("ChannelJoin: err=%v got=%#v", err, got)
	}
	if len(j.Encode()) != ChannelJoinLen {
		t.Errorf("ChannelJoin len = %d, want %d", len(j.Encode()), ChannelJoinLen)
	}
	a := &ChannelAccept{}
	a.Tag[1] = 5
	if got, err := DecodeChannelAccept(a.Encode()); err != nil || !reflect.DeepEqual(got, a) {
		t.Errorf("ChannelAccept: err=%v got=%#v", err, got)
	}

	// Bad magic / length → E5003.
	if _, err := DecodeClientHello(make([]byte, ClientHelloLen)); fault.GetCode(err) != fault.E5003 {
		t.Errorf("bad magic: code = %v, want E5003", fault.GetCode(err))
	}
	if _, err := DecodeServerHello(nil); fault.GetCode(err) != fault.E5003 {
		t.Errorf("short SERVER_HELLO: code = %v, want E5003", fault.GetCode(err))
	}
}

// Z-WIRE-01: the frame decoder never panics or hangs on arbitrary input.
func FuzzFrameDecode(f *testing.F) {
	for _, m := range sampleMessages() {
		if frame, err := Marshal(m); err == nil {
			f.Add(frame)
		}
	}
	f.Add([]byte{})
	f.Add([]byte{0x12, 0x00, 0x00, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := Unmarshal(b)
		if err == nil && m == nil {
			t.Fatal("nil message and nil error")
		}
		if err == nil {
			// A successful decode must re-marshal without panic.
			_, _ = Marshal(m)
		}
	})
}

// Z-WIRE-01 companion: the handshake decoders never panic on arbitrary input.
func FuzzHandshakeDecode(f *testing.F) {
	f.Add([]byte(Magic))
	f.Add(make([]byte, 78))
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = DecodeClientHello(b)
		_, _ = DecodeServerHello(b)
		_, _ = DecodeServerConfirm(b)
		_, _ = DecodeClientConfirm(b)
		_, _ = DecodeChannelJoin(b)
		_, _ = DecodeChannelAccept(b)
	})
}
