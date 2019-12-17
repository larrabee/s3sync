# Build in Docker container
FROM golang:1.13 as builder

COPY . /src/s3sync
WORKDIR /src/s3sync
RUN go mod vendor && \
    go build -o /usr/local/bin/s3sync ./cli

# Create s3sync image
FROM debian:10
COPY --from=builder /usr/local/bin/s3sync /usr/local/bin/s3sync
