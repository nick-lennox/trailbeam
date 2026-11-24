package trailbeamsqs

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// receiptHasher computes the keyed receipt-handle hash of contract section 7.
type receiptHasher struct {
	key   []byte
	keyID string
}

func newReceiptHasher(key string) *receiptHasher {
	h := &receiptHasher{key: []byte(key)}
	h.keyID = h.sum("trailbeam key id")[:8]
	return h
}

func (h *receiptHasher) sum(s string) string {
	m := hmac.New(sha256.New, h.key)
	m.Write([]byte(s))
	return hex.EncodeToString(m.Sum(nil))
}

// hash is lower_hex(HMAC-SHA256(key, receiptHandle))[0:32].
func (h *receiptHasher) hash(receiptHandle string) string { return h.sum(receiptHandle)[:32] }

// String never shows the key.
func (h *receiptHasher) String() string { return "receiptHasher(key_id=" + h.keyID + ")" }
