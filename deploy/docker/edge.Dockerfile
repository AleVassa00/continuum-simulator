FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./

RUN go mod download

COPY internal ./internal
COPY cmd/edge ./cmd/edge
COPY cmd/partition-coordinator ./cmd/partition-coordinator

RUN CGO_ENABLED=0 GOOS=linux go build \
    -o /out/edge \
    ./cmd/edge

RUN CGO_ENABLED=0 GOOS=linux go build \
    -o /out/partition-coordinator \
    ./cmd/partition-coordinator


FROM alpine:3.22

RUN adduser \
    -D \
    -H \
    -u 10001 \
    edge

USER edge

COPY --from=builder /out/edge /app/edge
COPY --from=builder /out/partition-coordinator /app/partition-coordinator

ENTRYPOINT ["/app/edge"]
