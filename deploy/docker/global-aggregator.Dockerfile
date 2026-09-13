FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./

RUN go mod download

COPY internal ./internal
COPY cmd/global-aggregator ./cmd/global-aggregator
COPY cmd/kafka-lag-collector ./cmd/kafka-lag-collector

RUN CGO_ENABLED=0 GOOS=linux go build \
    -o /out/global-aggregator \
    ./cmd/global-aggregator

RUN CGO_ENABLED=0 GOOS=linux go build \
    -o /out/kafka-lag-collector \
    ./cmd/kafka-lag-collector


FROM alpine:3.22

RUN apk add --no-cache ca-certificates postgresql17-client \
    && mkdir -p /app \
    && wget -q -O /app/rds-ca-bundle.pem \
       https://truststore.pki.rds.amazonaws.com/us-east-1/us-east-1-bundle.pem \
    && test -s /app/rds-ca-bundle.pem

# Honored by both pgx and psql; verify-full remains the sink's default.
ENV PGSSLROOTCERT=/app/rds-ca-bundle.pem
COPY --chmod=0444 deploy/postgres/global_aggregates.sql /app/global_aggregates.sql

RUN adduser \
    -D \
    -H \
    -u 10001 \
    global

USER global

COPY --from=builder /out/global-aggregator /app/global-aggregator
COPY --from=builder /out/kafka-lag-collector /app/kafka-lag-collector

ENTRYPOINT ["/app/global-aggregator"]
