package storage

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
)

func IsErrNotExist(err error) bool {
	var aErr awserr.Error
	if errors.As(err, &aErr) {
		if (aErr.Code() == s3.ErrCodeNoSuchKey) || (aErr.Code() == "NotFound") {
			return true
		}
	}

	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	return false
}

func IsErrPermission(err error) bool {
	var aErr awserr.Error
	if errors.As(err, &aErr) {
		if aErr.Code() == "AccessDenied" {
			return true
		}
	}

	if errors.Is(err, os.ErrPermission) {
		return true
	}
	return false
}

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
