package s3

import (
	"github.com/aws/aws-sdk-go/aws/request"
)

func withAcceptEncoding(e string) request.Option {
	return func(r *request.Request) {
		r.HTTPRequest.Header.Add("Accept-Encoding", e)
	}
}
