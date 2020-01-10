package main

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
)

func isErrNotExist(err error) bool {
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

func isErrPermission(err error) bool {
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
