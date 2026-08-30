# Stage 1: Build
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Cache the module download layer separately from the source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binary so the runtime stage needs no libc.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /app/ingestion-service ./cmd/ingestion-service

# Stage 2: Production
FROM alpine:3.20

WORKDIR /app

# Certificates for the outbound HTTPS calls to graph.facebook.com,
# api.anthropic.com, and the geocoder.
# The tzdata database is embedded in the binary, so no tzdata package is needed.
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 ingestion-service

COPY --from=builder /app/ingestion-service /app/ingestion-service

USER ingestion-service

EXPOSE 8082

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8082/health || exit 1

CMD ["/app/ingestion-service"]
