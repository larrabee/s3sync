package storage

import (
	"path"
	"strings"
)

// CleanPrefix removes slashes at the beginning and end of a string
func CleanPrefix(prefix string) string {
	prefix = strings.TrimPrefix(prefix, "/")
	prefix = strings.TrimSuffix(prefix, "/")
	return prefix
}

// JoinPrefix same as path.Join, but it saves the trailing slash of the last element.
// It is necessary because objects in S3 may have a name ending with a slash (like "object/with/slash/")
func JoinPrefix(elem ...string) string {
	result := path.Join(elem...)
	if strings.HasSuffix(elem[len(elem)-1], "/") {
		result += "/"
	}
	return result
}

// StrongEtag remove "W/" prefix from ETag.
// In some cases S3 return ETag with "W/" prefix which mean that it not strong ETag.
// For easier compare we remove this prefix.
func StrongEtag(s *string) *string {
	etag := strings.TrimPrefix(*s, "W/")
	return &etag
}
