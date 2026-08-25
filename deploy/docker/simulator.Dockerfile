FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./

RUN go mod download

COPY internal ./internal
COPY cmd/simulator ./cmd/simulator

RUN CGO_ENABLED=0 GOOS=linux go build \
    -o /out/simulator \
    ./cmd/simulator


FROM alpine:3.22

RUN adduser \
    -D \
    -H \
    -u 10001 \
    simulator

WORKDIR /app

USER simulator

COPY --from=builder /out/simulator /app/simulator

ENTRYPOINT ["/app/simulator"]
