package pipeline

import "fmt"

type PipelineError struct {
	StepName string
	Err      error
}

func (e *PipelineError) Error() string {
	return fmt.Sprintf("pipeline step: %s failed with error: %s", e.StepName, e.Err.Error())
}
