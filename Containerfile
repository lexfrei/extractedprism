# renovate: datasource=docker depName=golang
FROM docker.io/library/golang:1.27-alpine@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc AS builder

ARG VERSION=development
ARG REVISION=development

WORKDIR /build

RUN echo "nobody:x:65534:65534:Nobody:/:" > /tmp/passwd

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.Version=${VERSION} -X main.Revision=${REVISION}" \
    -trimpath \
    -o extractedprism \
    ./cmd/extractedprism

# scratch is a built-in empty pseudo-image with no OCI manifest or digest to pin.
FROM scratch

LABEL org.opencontainers.image.source="https://github.com/lexfrei/extractedprism" \
      org.opencontainers.image.title="extractedprism" \
      org.opencontainers.image.description="Per-node TCP load balancer for Kubernetes API server high availability" \
      org.opencontainers.image.licenses="BSD-3-Clause"

COPY --from=builder /tmp/passwd /etc/passwd
COPY --from=builder --chmod=555 /build/extractedprism /extractedprism

USER 65534

EXPOSE 7445/tcp 7446/tcp

ENTRYPOINT ["/extractedprism"]
