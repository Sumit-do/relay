# syntax=docker/dockerfile:1

# ---- build stage -----------------------------------------------------------
FROM golang:1.24-alpine AS build
ARG VERSION=dev
WORKDIR /src

# Cache module downloads separately from source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# modernc.org/sqlite is pure Go, so the binary is fully static.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/relay ./cmd/relay

# ---- runtime stage ---------------------------------------------------------
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata wget \
 && addgroup -S relay && adduser -S -G relay -h /data relay \
 && mkdir -p /data && chown relay:relay /data

COPY --from=build /out/relay /usr/local/bin/relay

USER relay
WORKDIR /data
VOLUME ["/data"]
EXPOSE 8080

ENV RELAY_DB=/data/relay.db \
    RELAY_ADDR=:8080 \
    RELAY_LOG_FORMAT=json

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s \
  CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null || exit 1

ENTRYPOINT ["relay"]
CMD ["serve"]
