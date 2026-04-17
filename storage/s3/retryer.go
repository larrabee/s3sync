package s3

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// Retryer implements aws.Retryer with a configurable retry count and fixed delay.
type Retryer struct {
	// RetryCnt is the maximum number of retries.
	RetryCnt uint

	// RetryInterval is the fixed delay between retries.
	RetryInterval time.Duration
}

// IsErrorRetryable returns true for any error when retries are enabled.
func (r *Retryer) IsErrorRetryable(_ error) bool {
	return r.RetryCnt > 0
}

// MaxAttempts returns the total number of attempts (retries + initial attempt).
func (r *Retryer) MaxAttempts() int {
	return int(r.RetryCnt) + 1
}

// RetryDelay returns the fixed delay for all retries.
func (r *Retryer) RetryDelay(_ int, _ error) (time.Duration, error) {
	return r.RetryInterval, nil
}

// GetInitialToken returns a no-op token release function.
func (r *Retryer) GetInitialToken() func(error) error {
	return nopReleaseToken
}

// GetRetryToken returns a no-op token release function.
func (r *Retryer) GetRetryToken(_ context.Context, _ error) (func(error) error, error) {
	return nopReleaseToken, nil
}

func nopReleaseToken(_ error) error { return nil }

// Ensure Retryer implements aws.Retryer at compile time.
var _ aws.Retryer = (*Retryer)(nil)
