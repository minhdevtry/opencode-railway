# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go bun_clienthello.bin ./

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o opencode-railway .

# Run stage
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata tor

WORKDIR /app
COPY --from=builder /app/opencode-railway /app/opencode-railway
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh


EXPOSE 8080

ENTRYPOINT ["/app/entrypoint.sh"]
