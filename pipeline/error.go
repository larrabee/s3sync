package pipeline

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/larrabee/s3sync/storage"
)

// PipelineError implement wrapper for pipeline errors.
type PipelineError struct {
	StepName string
	StepNum  int
	Err      error
}

func (e *PipelineError) Error() string {
	return fmt.Sprintf("pipeline step: %d (%s) failed with error: %s", e.StepNum, e.StepName, e.Err.Error())
}

// Unwrap PipelineError error and return the underlying error.
func (e *PipelineError) Unwrap() error {
	return e.Err
}

// StepConfigurationError raises when step have interface typing error.
type StepConfigurationError struct {
	StepName string
	StepNum  int
	Err      error
}

func (e *StepConfigurationError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("pipeline step: %d (%s) invalid configuration passed", e.StepNum, e.StepName)
	}
	return fmt.Sprintf("pipeline step: %d (%s) configuration error: %s", e.StepNum, e.StepName, e.Err)
}

// Unwrap StepConfigurationError error and return the underlying error.
func (e *StepConfigurationError) Unwrap() error {
	return e.Err
}

// ObjectError contain a pointer to an Object that failed with error
type ObjectError struct {
	Object *storage.Object
	Err    error
}

func (e *ObjectError) Error() string {
	return fmt.Sprintf("object: %s sync error: %s", *e.Object.Key, e.Err)
}

// Unwrap ObjectError error and return the underlying error.
func (e *ObjectError) Unwrap() error {
	return e.Err
}

// IsContextCancelErr check that input error is caused by a context cancellation.
func IsContextCancelErr(err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
	}

	var aErr awserr.Error
	if errors.As(err, &aErr) && aErr.OrigErr() == context.Canceled {
		return true
	}

	return false
}
