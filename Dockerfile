# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.26.4 AS builder
WORKDIR /workspace

ARG TARGETOS
ARG TARGETARCH

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /manager ./cmd/manager

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=builder /manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
