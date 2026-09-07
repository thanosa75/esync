package paircode

import (
	"hash/crc32"
	"math/rand"
	"net/netip"
	"strings"
	"testing"

	"esync/internal/fault"
	"esync/internal/obs"
)

func mustAddr(s string) netip.Addr {
	a, err := netip.ParseAddr(s)
	if err != nil {
		panic(err)
	}
	return a
}

// fixedSecret is the deterministic 128-bit secret for the golden vectors.
var fixedSecret = obs.NewSecret([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})

func goldenPayloads() map[string]*Payload {
	return map[string]*Payload{
		"one-v4": {
			Endpoints: []Endpoint{{mustAddr("192.168.1.24"), 49721}},
			Secret:    fixedSecret,
		},
		"two-mixed": {
			Endpoints: []Endpoint{
				{mustAddr("10.0.0.5"), 5000},
				{mustAddr("fd12:3456::1"), 5000},
			},
			Secret: fixedSecret,
		},
		"four-mixed": {
			Endpoints: []Endpoint{
				{mustAddr("192.168.1.24"), 49721},
				{mustAddr("10.0.0.5"), 49721},
				{mustAddr("fd00::1"), 49721},
				{mustAddr("2001:db8::1"), 49721},
			},
			Secret:     fixedSecret,
			Restricted: false,
		},
	}
}

// G-PAIR-01 / T-PAIR-01: payload → code string, including the CRC bytes.
var goldenCodes = map[string]string{
	"one-v4":     "04009-G5804-CC4E8-00410-61050-R3GG2-8A1C6-0T3GF-98RV1-A8",
	"two-mixed":  "05008-2G000-2H720-6ZM93-8NG00-00000-00000-00000-049RG-00108-1G818-60W40-J2GB1-G6GW3-R1SPA-5E",
	"four-mixed": "07009-G5804-CC4E8-41800-01E27-43FT0-00000-00000-00000-00000-003GH-S0RG0-23DR0-00000-00000-00000-000W4-E8004-10610-50R3G-G28A1-C60T3-GF7W5-XB5R",
}

func TestGoldenPairCodes(t *testing.T) {
	for name, p := range goldenPayloads() {
		got := Encode(p)
		want := goldenCodes[name]
		if want == "" {
			t.Errorf("%s: no golden code; actual = %q", name, got)
			continue
		}
		if got != want {
			t.Errorf("%s:\n got  %q\n want %q", name, got, want)
		}
		// Round-trips back to an equal payload.
		dec, err := Decode(got)
		if err != nil {
			t.Fatalf("%s: decode: %v", name, err)
		}
		assertPayloadEqual(t, name, dec, p)
	}
}

func assertPayloadEqual(t *testing.T, name string, got, want *Payload) {
	t.Helper()
	if len(got.Endpoints) != len(want.Endpoints) {
		t.Fatalf("%s: %d endpoints, want %d", name, len(got.Endpoints), len(want.Endpoints))
	}
	for i := range got.Endpoints {
		if got.Endpoints[i].Addr != want.Endpoints[i].Addr || got.Endpoints[i].Port != want.Endpoints[i].Port {
			t.Errorf("%s: endpoint %d = %v, want %v", name, i, got.Endpoints[i], want.Endpoints[i])
		}
	}
	if string(got.Secret.Bytes()) != string(want.Secret.Bytes()) {
		t.Errorf("%s: secret mismatch", name)
	}
	if got.Restricted != want.Restricted {
		t.Errorf("%s: restricted = %v, want %v", name, got.Restricted, want.Restricted)
	}
}

// T-PAIR-02: the single-IPv4 code is 47 base32 characters, within REQ-PAIR-002's
// 64-character budget.
func TestSingleV4CodeLength(t *testing.T) {
	code := Encode(goldenPayloads()["one-v4"])
	bare := strings.ReplaceAll(code, "-", "")
	if len(bare) != 47 {
		t.Errorf("single-v4 code is %d chars, want 47", len(bare))
	}
	if len(bare) > 64 {
		t.Errorf("code exceeds the 64-char budget")
	}
}

// T-PAIR-03: aliases (I/L → 1, O → 0) and lower case decode identically.
func TestAliasDecode(t *testing.T) {
	canonical := Encode(goldenPayloads()["one-v4"])
	base, err := Decode(canonical)
	if err != nil {
		t.Fatal(err)
	}

	lower := strings.ToLower(canonical)
	// Substitute alias characters for their canonical equivalents.
	aliased := strings.NewReplacer("1", "I", "0", "O").Replace(canonical)
	spaced := strings.ReplaceAll(canonical, "-", " \n")

	for _, variant := range []string{lower, aliased, spaced} {
		dec, err := Decode(variant)
		if err != nil {
			t.Fatalf("variant %q: %v", variant, err)
		}
		assertPayloadEqual(t, "alias", dec, base)
	}
}

// T-PAIR-04: the CRC catches every single-character substitution and every
// adjacent transposition over a sample of codes.
func TestCRCCatchesTypos(t *testing.T) {
	var samples []string
	for _, p := range goldenPayloads() {
		samples = append(samples, strings.ReplaceAll(Encode(p), "-", ""))
	}
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 20; i++ {
		var sec [16]byte
		rng.Read(sec[:])
		p := &Payload{
			Endpoints: []Endpoint{{mustAddr("192.168.0.1"), uint16(1024 + rng.Intn(60000))}},
			Secret:    obs.NewSecret(sec[:]),
		}
		samples = append(samples, strings.ReplaceAll(Encode(p), "-", ""))
	}

	for _, code := range samples {
		// Single-character substitution.
		for pos := 0; pos < len(code); pos++ {
			for a := 0; a < len(Alphabet); a++ {
				if Alphabet[a] == code[pos] {
					continue
				}
				bad := code[:pos] + string(Alphabet[a]) + code[pos+1:]
				if _, err := Decode(bad); err == nil {
					t.Errorf("substitution at %d (%c→%c) not detected in %s", pos, code[pos], Alphabet[a], code)
				}
			}
		}
		// Adjacent transposition of differing characters.
		for pos := 0; pos+1 < len(code); pos++ {
			if code[pos] == code[pos+1] {
				continue
			}
			bad := code[:pos] + string(code[pos+1]) + string(code[pos]) + code[pos+2:]
			if _, err := Decode(bad); err == nil {
				t.Errorf("transposition at %d not detected in %s", pos, code)
			}
		}
	}
}

// T-PAIR-05: an unknown version byte is E2002, distinct from a corrupt code.
func TestUnknownVersion(t *testing.T) {
	p := goldenPayloads()["one-v4"]
	// Rebuild the payload with version 0x02 and a valid CRC, then encode.
	raw := decodeToRaw(t, Encode(p))
	raw[0] = 0x02
	fixCRC(raw)
	code := group(b32.EncodeToString(raw))
	if _, err := Decode(code); fault.GetCode(err) != fault.E2002 {
		t.Errorf("code = %v, want E2002", fault.GetCode(err))
	}
}

// T-PAIR-08: --bind yields exactly that endpoint and the restricted bit survives
// an encode/decode round trip (REQ-SEC-016).
func TestRestrictedBind(t *testing.T) {
	eps, err := Discover("192.168.5.9")
	if err != nil {
		t.Fatalf("Discover(bind): %v", err)
	}
	if len(eps) != 1 || eps[0].Addr != mustAddr("192.168.5.9") {
		t.Fatalf("Discover(bind) = %v, want the single bound address", eps)
	}
	eps[0].Port = 40000
	p := &Payload{Endpoints: eps, Secret: fixedSecret, Restricted: true}
	dec, err := Decode(Encode(p))
	if err != nil {
		t.Fatal(err)
	}
	if !dec.Restricted {
		t.Errorf("restricted bit lost across encode/decode")
	}

	if _, err := Discover("not-an-ip"); fault.GetCode(err) != fault.E1005 {
		t.Errorf("Discover(bad bind) = %v, want E1005", fault.GetCode(err))
	}
}

// T-PAIR-06: discovery ranking prefers global/private IPv4 over IPv6, and IPv6
// global/ULA over link-local. Uses the internal score function directly since a
// unit test cannot control the host's interface list.
func TestDiscoveryRanking(t *testing.T) {
	pubV4 := score(mustAddr("203.0.113.7"), true)
	privV4wired := score(mustAddr("192.168.1.10"), true)
	privV4wifi := score(mustAddr("192.168.1.10"), false)
	ula := score(mustAddr("fd00::1"), true)
	globV6 := score(mustAddr("2001:db8::1"), true)
	llV6 := score(mustAddr("fe80::1"), true)
	v4ll := score(mustAddr("169.254.1.1"), true)

	if !(pubV4 > privV4wired && privV4wired > privV4wifi && privV4wifi > ula) {
		t.Errorf("v4 ranking wrong: pub=%d privWired=%d privWifi=%d ula=%d", pubV4, privV4wired, privV4wifi, ula)
	}
	if !(ula == globV6 && globV6 > llV6) {
		t.Errorf("v6 ranking wrong: ula=%d glob=%d ll=%d", ula, globV6, llV6)
	}
	if v4ll != 0 {
		t.Errorf("IPv4 link-local should not be advertised, score=%d", v4ll)
	}

	// Discover on the real host must not error (loopback-only CI is the exception).
	if eps, err := Discover(""); err != nil {
		t.Logf("Discover() on this host: %v (no non-loopback interface?)", err)
	} else if len(eps) > maxEndpoints {
		t.Errorf("Discover returned %d endpoints, max is %d", len(eps), maxEndpoints)
	}
}

func TestDerivedIDs(t *testing.T) {
	sid1 := SessionID(fixedSecret)
	sid2 := SessionID(fixedSecret)
	if sid1 != sid2 {
		t.Fatal("SessionID not deterministic")
	}
	other := SessionID(obs.NewSecret([]byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}))
	if sid1 == other {
		t.Error("SessionID collision on distinct secrets")
	}
	k := KPair(fixedSecret)
	if len(k.Bytes()) != 32 {
		t.Errorf("KPair length = %d, want 32", len(k.Bytes()))
	}
	if k.String() != "[redacted]" {
		t.Errorf("KPair must render redacted, got %q", k.String())
	}
}

// helpers for the version test

func decodeToRaw(t *testing.T, code string) []byte {
	t.Helper()
	raw, err := b32.DecodeString(clean(code))
	if err != nil {
		t.Fatalf("decodeToRaw: %v", err)
	}
	return raw
}

func fixCRC(raw []byte) {
	c := crc32.Checksum(raw[:len(raw)-4], crcTable)
	raw[len(raw)-4] = byte(c >> 24)
	raw[len(raw)-3] = byte(c >> 16)
	raw[len(raw)-2] = byte(c >> 8)
	raw[len(raw)-1] = byte(c)
}

// Z-PAIR-01: the code decoder never panics or hangs on arbitrary strings.
func FuzzPairDecode(f *testing.F) {
	for _, p := range goldenPayloads() {
		f.Add(Encode(p))
	}
	f.Add("")
	f.Add("----")
	f.Add(strings.Repeat("Z", 300))
	f.Fuzz(func(t *testing.T, s string) {
		_, _ = Decode(s)
	})
}

// Z-PAIR-02: the payload decoder never panics on arbitrary payload bytes.
func FuzzPayloadDecode(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 29))
	f.Add([]byte{Version, 0x00})
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = Decode(b32.EncodeToString(b))
	})
}
