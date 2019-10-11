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
