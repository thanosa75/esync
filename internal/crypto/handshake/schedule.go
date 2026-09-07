package handshake

import (
	"crypto/hkdf"
	"crypto/sha256"

	"esync/internal/fault"
	"esync/internal/obs"
)

// Key schedule (ARCHITECTURE §6.3):
//
//	PRK           = HKDF-Extract(salt = nonce_c||nonce_s, ikm = Z||K_pair)
//	K_conf        = HKDF-Expand(PRK, "esync/v1 confirm",       32)
//	K_chan        = HKDF-Expand(PRK, "esync/v1 channel-auth",  32)
//	K_enc[dir][ch]= HKDF-Expand(PRK, "esync/v1 enc " ||dir||ch, 32)
//	K_mac[dir][ch]= HKDF-Expand(PRK, "esync/v1 mac " ||dir||ch, 32)
//
// dir is the 3-byte string "s2r" or "r2s"; ch is a single u8 byte.

const (
	infoConfirm     = "esync/v1 confirm"
	infoChannelAuth = "esync/v1 channel-auth"
	infoEncPrefix   = "esync/v1 enc " // trailing space is significant
	infoMacPrefix   = "esync/v1 mac " // trailing space is significant
)

type schedule struct {
	prk   []byte
	kConf obs.Secret
	kChan obs.Secret
}

func newSchedule(nonceC, nonceS, z, kPair []byte) (*schedule, error) {
	salt := make([]byte, 0, len(nonceC)+len(nonceS))
	salt = append(append(salt, nonceC...), nonceS...)
	ikm := make([]byte, 0, len(z)+len(kPair))
	ikm = append(append(ikm, z...), kPair...)

	prk, err := hkdf.Extract(sha256.New, ikm, salt)
	if err != nil {
		return nil, fault.Wrap(fault.E4001, "hkdf extract", "", err)
	}
	kConf, err := expand(prk, infoConfirm)
	if err != nil {
		return nil, err
	}
	kChan, err := expand(prk, infoChannelAuth)
	if err != nil {
		return nil, err
	}
	return &schedule{prk: prk, kConf: obs.NewSecret(kConf), kChan: obs.NewSecret(kChan)}, nil
}

func (s *schedule) recordKeys(dir Direction, ch uint8) (kEnc, kMac obs.Secret, err error) {
	suffix := dir.String() + string([]byte{ch})
	e, err := expand(s.prk, infoEncPrefix+suffix)
	if err != nil {
		return obs.Secret{}, obs.Secret{}, err
	}
	m, err := expand(s.prk, infoMacPrefix+suffix)
	if err != nil {
		return obs.Secret{}, obs.Secret{}, err
	}
	return obs.NewSecret(e), obs.NewSecret(m), nil
}

func expand(prk []byte, info string) ([]byte, error) {
	k, err := hkdf.Expand(sha256.New, prk, info, 32)
	if err != nil {
		return nil, fault.Wrap(fault.E4001, "hkdf expand", info, err)
	}
	return k, nil
}
