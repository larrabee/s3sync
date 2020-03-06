package storage

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
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

func IsAwsContextCanceled(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) {
		return true
	}

	var aErr awserr.Error
	if ok := errors.As(err, &aErr); ok && aErr.OrigErr() == context.Canceled {
		return true
	} else if ok && aErr.Code() == request.CanceledErrorCode {
		return true
	}

	return false
}
