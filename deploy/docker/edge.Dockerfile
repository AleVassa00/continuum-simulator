FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./

RUN go mod download

COPY internal ./internal
COPY cmd/edge ./cmd/edge

RUN CGO_ENABLED=0 GOOS=linux go build \
    -o /out/edge \
    ./cmd/edge


FROM alpine:3.22

RUN adduser \
    -D \
    -H \
    -u 10001 \
    edge

USER edge

COPY --from=builder /out/edge /app/edge

ENTRYPOINT ["/app/edge"]