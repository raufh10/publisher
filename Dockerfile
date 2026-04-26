# Stage 1: Build the binary
FROM golang:1.26.1-alpine AS builder

WORKDIR /app

# Install git for private modules if needed
RUN apk add --no-cache git

# Copy module files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY cmd/ ./cmd/
COPY internal/ ./internal/

# Build the publisher binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /publisher ./cmd/publisher/main.go

# Stage 2: Final lightweight image
FROM alpine:latest

# Install certs for NATS TLS connections and tzdata
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app/

# Copy the binary from the builder stage
COPY --from=builder /publisher .

# Set the binary as the entrypoint
ENTRYPOINT ["./publisher"]
