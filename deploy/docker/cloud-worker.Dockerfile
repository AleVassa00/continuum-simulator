FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./

RUN go mod download

COPY internal ./internal
COPY cmd/cloud-worker ./cmd/cloud-worker

RUN CGO_ENABLED=0 GOOS=linux go build \
    -o /out/cloud-worker \
    ./cmd/cloud-worker


FROM alpine:3.22

RUN adduser \
    -D \
    -H \
    -u 10001 \
    cloud

USER cloud

COPY --from=builder /out/cloud-worker /app/cloud-worker

ENTRYPOINT ["/app/cloud-worker"]