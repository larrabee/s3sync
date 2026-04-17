package s3

import (
	"context"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func withAcceptEncoding(encoding string) func(*awss3.Options) {
	return func(o *awss3.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Build.Add(
				middleware.BuildMiddlewareFunc("AddAcceptEncoding",
					func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
						if req, ok := in.Request.(*smithyhttp.Request); ok {
							req.Header.Set("Accept-Encoding", encoding)
						}
						return next.HandleBuild(ctx, in)
					}),
				middleware.After,
			)
		})
	}
}
