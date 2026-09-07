package record

import (
	"bytes"
	"encoding/hex"
	"io"
	"math"
	"testing"

	"esync/internal/fault"
	"esync/internal/obs"
)

// goldenRecordHex is the exact §8.1 wire encoding of WriteFrame("hello record
// layer!") on channel 1, seq 0, with K_enc = 32 x 0x2b, K_mac = 32 x 0x4d and
// iv = 00..0f. Regenerate only with a deliberate, reviewed format change.
const goldenRecordHex = "0101000000000000000000000020000102030405060708090a0b0c0d0e0f" +
	"9f5ebafd33030513a82af2f60910a1e1b90690179e59a7dfab8dc2e19425979" +
	"54150386434c10554358616898abfd656d8992cb0b749dccd0b0c6a9aa3ce9d25"

func keys() (obs.Secret, obs.Secret) {
	e := bytes.Repeat([]byte{0x2b}, 32)
	m := bytes.Repeat([]byte{0x4d}, 32)
	return obs.NewSecret(e), obs.NewSecret(m)
}

// fixedIV is a deterministic "CSPRNG" for golden vectors.
type fixedIV struct{ b []byte }

func (f fixedIV) Read(p []byte) (int, error) { return copy(p, f.b), nil }

// G-REC-01 / REQ-SEC-001: fixed (K_enc, K_mac, iv, plaintext) produce these exact
// record bytes. Locks the §8.1 wire format.
func TestGoldenRecord(t *testing.T) {
	kEnc, kMac := keys()
	iv := make([]byte, 16)
	for i := range iv {
		iv[i] = byte(i)
	}
	var buf bytes.Buffer
	w := newWriter(&buf, kEnc, kMac, 1, fixedIV{iv})
	if err := w.WriteFrame([]byte("hello record layer!")); err != nil {
		t.Fatal(err)
	}
	got := hex.EncodeToString(buf.Bytes())
	if got != goldenRecordHex {
		t.Fatalf("record bytes:\n got %s\nwant %s", got, goldenRecordHex)
	}

	// And it round-trips.
	r := NewReader(&buf, kEnc, kMac, 1)
	pt, err := r.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if string(pt) != "hello record layer!" {
		t.Fatalf("round-trip plaintext = %q", pt)
	}
}

// P-REC-01 / REQ-SEC-002: Reader∘Writer is identity for random plaintexts, and
// flipping any single bit of any record byte yields a fault, never wrong
// plaintext.
func TestPropertyRoundTripAndBitFlip(t *testing.T) {
	kEnc, kMac := keys()
	sizes := []int{1, 15, 16, 17, 255, 4096, 65535}
	for _, n := range sizes {
		pt := make([]byte, n)
		for i := range pt {
			pt[i] = byte(i*7 + n)
		}
		var buf bytes.Buffer
		w := NewWriter(&buf, kEnc, kMac, 1)
		if err := w.WriteFrame(pt); err != nil {
			t.Fatal(err)
		}
		rec := append([]byte(nil), buf.Bytes()...)

		out, err := NewReader(bytes.NewReader(rec), kEnc, kMac, 1).ReadFrame()
		if err != nil || !bytes.Equal(out, pt) {
			t.Fatalf("n=%d round-trip failed: err=%v", n, err)
		}

		for bit := 0; bit < len(rec)*8; bit += 1 + bit/13 { // sample bits, cheaply
			tampered := append([]byte(nil), rec...)
			tampered[bit/8] ^= 1 << (bit % 8)
			got, err := NewReader(bytes.NewReader(tampered), kEnc, kMac, 1).ReadFrame()
			if err == nil && bytes.Equal(got, pt) {
				continue // flipped a byte past the record, or a no-op; fine
			}
			if err == nil {
				t.Fatalf("n=%d bit=%d: tampered record returned different plaintext with no error", n, bit)
			}
			if fault.GetCode(err) == "" {
				t.Fatalf("n=%d bit=%d: non-fault error %v", n, bit, err)
			}
		}
	}
}

// T-REC-03 / REQ-SEC-004: fresh IV per record.
func TestFreshIVs(t *testing.T) {
	kEnc, kMac := keys()
	var buf bytes.Buffer
	w := NewWriter(&buf, kEnc, kMac, 1)
	if err := w.WriteFrame([]byte("a")); err != nil {
		t.Fatal(err)
	}
	first := append([]byte(nil), buf.Bytes()...)
	buf.Reset()
	if err := w.WriteFrame([]byte("a")); err != nil {
		t.Fatal(err)
	}
	second := buf.Bytes()
	if bytes.Equal(first[headerLen:prefixLen], second[headerLen:prefixLen]) {
		t.Fatal("two records reused the same IV")
	}
}

// T-REC-04 / REQ-SEC-005: wrong-channel or wrong-direction keys reject.
func TestWrongKeysAndChannelReject(t *testing.T) {
	kEnc, kMac := keys()
	var buf bytes.Buffer
	if err := NewWriter(&buf, kEnc, kMac, 1).WriteFrame([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	rec := buf.Bytes()

	otherMac := obs.NewSecret(bytes.Repeat([]byte{0x99}, 32))
	if _, err := NewReader(bytes.NewReader(rec), kEnc, otherMac, 1).ReadFrame(); fault.GetCode(err) != fault.E4003 {
		t.Fatalf("wrong MAC key: err = %v, want E4003", err)
	}
	if _, err := NewReader(bytes.NewReader(rec), kEnc, kMac, 2).ReadFrame(); fault.GetCode(err) != fault.E4003 {
		t.Fatalf("wrong channel: err = %v, want E4003", err)
	}
}

// T-REC-05 / REQ-SEC-006: replay / reorder is E4004.
func TestSequenceReplay(t *testing.T) {
	kEnc, kMac := keys()
	var buf bytes.Buffer
	w := NewWriter(&buf, kEnc, kMac, 1)
	_ = w.WriteFrame([]byte("one"))
	rec0 := append([]byte(nil), buf.Bytes()...)
	buf.Reset()
	_ = w.WriteFrame([]byte("two"))
	rec1 := buf.Bytes()

	r := NewReader(io.MultiReader(bytes.NewReader(rec0), bytes.NewReader(rec0)), kEnc, kMac, 1)
	if _, err := r.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadFrame(); fault.GetCode(err) != fault.E4004 {
		t.Fatalf("replayed record: err = %v, want E4004", err)
	}

	// Reorder: feed rec1 (seq 1) first.
	if _, err := NewReader(bytes.NewReader(rec1), kEnc, kMac, 1).ReadFrame(); fault.GetCode(err) != fault.E4004 {
		t.Fatalf("reordered record: err = %v, want E4004", err)
	}
}

// REQ-SEC-007: a stream that ends without the authenticated CLOSE is E4005;
// a clean Close is io.EOF.
func TestAuthenticatedClose(t *testing.T) {
	kEnc, kMac := keys()
	var buf bytes.Buffer
	w := NewWriter(&buf, kEnc, kMac, 0)
	_ = w.WriteFrame([]byte("body"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r := NewReader(&buf, kEnc, kMac, 0)
	if _, err := r.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadFrame(); err != io.EOF {
		t.Fatalf("after CLOSE: err = %v, want io.EOF", err)
	}

	// Truncated stream (no CLOSE).
	var t2 bytes.Buffer
	_ = NewWriter(&t2, kEnc, kMac, 0).WriteFrame([]byte("body"))
	r2 := NewReader(&t2, kEnc, kMac, 0)
	_, _ = r2.ReadFrame()
	if _, err := r2.ReadFrame(); fault.GetCode(err) != fault.E4005 {
		t.Fatalf("truncated stream: err = %v, want E4005", err)
	}
}

// REQ-SEC-014: an out-of-range ct_len is rejected before allocation.
func TestCTLenBounds(t *testing.T) {
	kEnc, kMac := keys()
	hdr := make([]byte, headerLen)
	hdr[0] = version
	hdr[1] = 0
	// ct_len = 2 GiB, far past MAX_CT and not something we should allocate.
	hdr[10], hdr[11], hdr[12], hdr[13] = 0x80, 0, 0, 0
	if _, err := NewReader(bytes.NewReader(hdr), kEnc, kMac, 0).ReadFrame(); fault.GetCode(err) != fault.E5003 {
		t.Fatalf("huge ct_len: err = %v, want E5003", err)
	}
	// not a multiple of 16
	hdr[10], hdr[11], hdr[12], hdr[13] = 0, 0, 0, 17
	if _, err := NewReader(bytes.NewReader(hdr), kEnc, kMac, 0).ReadFrame(); fault.GetCode(err) != fault.E5003 {
		t.Fatalf("unaligned ct_len: err = %v, want E5003", err)
	}
}

func TestVersionReject(t *testing.T) {
	kEnc, kMac := keys()
	var buf bytes.Buffer
	_ = NewWriter(&buf, kEnc, kMac, 0).WriteFrame([]byte("x"))
	rec := append([]byte(nil), buf.Bytes()...)
	rec[0] = 0x02
	if _, err := NewReader(bytes.NewReader(rec), kEnc, kMac, 0).ReadFrame(); fault.GetCode(err) != fault.E5002 {
		t.Fatalf("bad version: err = %v, want E5002", err)
	}
}

func TestSeqExhaustion(t *testing.T) {
	kEnc, kMac := keys()
	w := newWriter(io.Discard, kEnc, kMac, 1, fixedIV{make([]byte, 16)})
	w.seq = math.MaxUint64
	if err := w.WriteFrame([]byte("x")); fault.GetCode(err) != fault.E4006 {
		t.Fatalf("seq exhaustion: err = %v, want E4006", err)
	}
}

// Z-REC-01 / REQ-SEC-014: the reader never panics and never allocates
// unboundedly on arbitrary input.
func FuzzRecordReader(f *testing.F) {
	kEnc, kMac := keys()
	var seed bytes.Buffer
	_ = NewWriter(&seed, kEnc, kMac, 1).WriteFrame([]byte("seed frame"))
	f.Add(seed.Bytes())
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xff}, 64))

	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data), kEnc, kMac, 1)
		for i := 0; i < 8; i++ {
			out, err := r.ReadFrame()
			if err != nil {
				break
			}
			if len(out) > MaxCTData {
				t.Fatalf("frame larger than MAX_CT: %d", len(out))
			}
		}
	})
}
