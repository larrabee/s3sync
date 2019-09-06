package pipeline

import (
	"fmt"
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
	} else {
		return fmt.Sprintf("pipeline step: %d (%s) configuration error: %s", e.StepNum, e.StepName, e.Err)
	}
}

func (e *StepConfigurationError) Unwrap() error {
	return e.Err
}

type ObjectError struct {
	Object *storage.Object
	Err    error
}

func (e *ObjectError) Error() string {
	return fmt.Sprintf("object: %s sync error: %s", *e.Object.Key, e.Err)
}

func (e *ObjectError) Unwrap() error {
	return e.Err
}
