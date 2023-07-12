# Build in Docker container
FROM golang:1.20.5-alpine as builder

ENV CGO_ENABLED 0
WORKDIR /src/s3sync
COPY . ./
RUN go mod vendor && \
    go build -o s3sync ./cli

# Create s3sync image
FROM alpine
RUN apk --no-cache add ca-certificates
COPY --link --from=builder /src/s3sync/s3sync /s3sync
ENTRYPOINT ["/s3sync"]
