# Build in Docker container
FROM golang:1.17.6 as builder

ENV CGO_ENABLED 0
WORKDIR /src/s3sync
COPY . ./
RUN go mod vendor && \
    go build -o s3sync ./cli

# Create s3sync image
FROM debian:buster-slim
RUN apt update && apt install -y ca-certificates
COPY --from=builder /src/s3sync/s3sync /s3sync
ENTRYPOINT ["/s3sync"]
