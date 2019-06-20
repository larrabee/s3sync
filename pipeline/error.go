package pipeline

import "fmt"

type PipelineError struct {
	StepName string
	StepNum  int
	Err      error
}

func (e *PipelineError) Error() string {
	return fmt.Sprintf("pipeline step: %d (%s) failed with error: %s", e.StepNum, e.StepName, e.Err.Error())
}

type StepConfigurationError struct {
	StepName string
	StepNum  int
}

func (e *StepConfigurationError) Error() string {
	return fmt.Sprintf("pipeline step: %d (%s) invalid configuration passed", e.StepNum, e.StepName)
}
