FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./

RUN go mod download

COPY internal ./internal
COPY cmd/global-aggregator ./cmd/global-aggregator

RUN CGO_ENABLED=0 GOOS=linux go build \
    -o /out/global-aggregator \
    ./cmd/global-aggregator


FROM alpine:3.22

RUN adduser \
    -D \
    -H \
    -u 10001 \
    global

USER global

COPY --from=builder /out/global-aggregator /app/global-aggregator

ENTRYPOINT ["/app/global-aggregator"]
