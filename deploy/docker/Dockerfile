FROM golang:1.25.6-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/simulator ./cmd/simulator && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/edge ./cmd/edge && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/cloud-worker ./cmd/cloud-worker

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S -g 10001 continuum && \
    adduser -S -D -H -u 10001 -G continuum continuum

WORKDIR /opt/continuum
COPY --from=build --chown=10001:10001 /out/ /opt/continuum/bin/

USER 10001:10001

