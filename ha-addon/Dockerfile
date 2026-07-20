ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:3.20
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /usr/bin/energy-optimiser .

FROM ${BUILD_FROM}

COPY --from=builder /usr/bin/energy-optimiser /usr/bin/energy-optimiser
COPY ha-addon/run.sh /etc/services.d/energy-optimiser/run

RUN chmod a+x /etc/services.d/energy-optimiser/run
