# Build in Docker container
FROM golang:1.13 as builder

ENV CGO_ENABLED 0
WORKDIR /src/s3sync
COPY . ./
RUN go mod vendor && \
    go build -o s3sync ./cli

# Create s3sync image
FROM scratch
COPY --from=builder /src/s3sync/s3sync /s3sync
ENTRYPOINT ["/s3sync"]