# Multi-stage Docker build for FlashDB
FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /flashdb-server ./cmd/flashdb-server
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /flashdb-cli ./cmd/flashdb-cli

# Production minimal runtime
FROM alpine:3.20

RUN apk --no-cache add ca-certificates
WORKDIR /data

COPY --from=builder /flashdb-server /usr/local/bin/flashdb-server
COPY --from=builder /flashdb-cli /usr/local/bin/flashdb-cli

EXPOSE 6379

ENTRYPOINT ["flashdb-server"]
CMD ["--host", "0.0.0.0", "--port", "6379", "--aof", "/data/appendonly.aof"]
