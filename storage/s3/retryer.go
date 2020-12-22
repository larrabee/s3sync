package s3

import (
	"github.com/aws/aws-sdk-go/aws/request"
	"time"
)

// Retryer implements basic retry logic
//You can implement the request.Retryer interface.
type Retryer struct {
	// RetryCnt is the number of max retries that will be performed.
	// By default, this is zero.
	RetryCnt uint

	// RetryDelay is the minimum retry delay after which retry will be performed.
	// If not set, the value is 0ns.
	RetryDelay time.Duration
}

// MaxRetries returns the number of maximum returns the service will use to make
// an individual API request.
func (d Retryer) MaxRetries() int {
	return int(d.RetryCnt)
}

// RetryRules returns the delay duration before retrying this request again
func (d Retryer) RetryRules(r *request.Request) time.Duration {

	// if number of max retries is zero, no retries will be performed.
	if d.RetryCnt == 0 {
		return 0
	}

	return d.RetryDelay
}

// ShouldRetry returns true if the request should be retried.
func (d Retryer) ShouldRetry(r *request.Request) bool {

	// ShouldRetry returns false if number of max retries is 0.
	if d.RetryCnt == 0 {
		return false
	}

	// If one of the other handlers already set the retry state
	// we don't want to override it based on the service's state
	if r.Retryable != nil {
		return *r.Retryable
	}

	return r.IsErrorRetryable() || r.IsErrorThrottle() || true
}
