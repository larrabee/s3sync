package storage

import (
	"math/rand"
	"strings"
	"time"
)

// StrongEtag remove "W/" prefix from ETag.
// In some cases S3 return ETag with "W/" prefix which mean that it not strong ETag.
// For easier compare we remove this prefix.
func StrongEtag(s *string) *string {
	etag := strings.TrimPrefix(*s, "W/")
	return &etag
}

// ErrHandlingMask is is a bitmask for storing error handling settings.
type ErrHandlingMask uint8

// Has checks if the flag is set in the bitmask.
func (f ErrHandlingMask) Has(flag ErrHandlingMask) bool { return f&flag != 0 }

// Add the flag to the bitmask.
func (f *ErrHandlingMask) Add(flag ErrHandlingMask) { *f |= flag }

const (
	HandleErrNotExist ErrHandlingMask = 1 << iota
	HandleErrPermission
	HandleErrOther = 64
)



const letterBytes = "abcdefghijklmnopqrstuvwxyz0123456789"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func GetInsecureRandString(n int) string {
	sb := strings.Builder{}
	sb.Grow(n)
	src := rand.NewSource(time.Now().UnixNano())
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			sb.WriteByte(letterBytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return sb.String()
}

