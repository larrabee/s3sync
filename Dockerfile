# Build in Docker container
FROM cgr.dev/chainguard/go:1.19.2@sha256:3f7206a2cfbf680b63f71188cd76c7597d35720a0a9a6c95fc5c9556ba74e332 as builder

ENV CGO_ENABLED 0
WORKDIR /src/s3sync
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN go build -o s3sync ./cli

# Create s3sync image
FROM cgr.dev/chainguard/glibc-dynamic:latest
COPY --from=builder /src/s3sync/s3sync /s3sync
ENTRYPOINT ["/s3sync"]
