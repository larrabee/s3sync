package storage

import (
	"context"
	"errors"
	"os"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/gophercloud/gophercloud"
)

func IsErrNotExist(err error) bool {
	var nsk *s3types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotFound" {
		return true
	}

	if errors.Is(err, os.ErrNotExist) {
		return true
	}

	var sErr gophercloud.ErrDefault404
	if errors.As(err, &sErr) {
		return true
	}

	return false
}

func IsErrPermission(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "AccessDenied" {
		return true
	}

	if errors.Is(err, os.ErrPermission) {
		return true
	}

	var sErr gophercloud.ErrDefault403
	if errors.As(err, &sErr) {
		return true
	}

	return false
}

func IsAwsContextCanceled(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, context.Canceled)
}
