package storage

import (
	"strings"
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
