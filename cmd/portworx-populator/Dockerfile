# Build stage
FROM golang:1.24 AS builder

WORKDIR /workspace

# Copy go mod files
COPY go.mod go.mod
COPY go.sum go.sum

# Cache deps before building and copying source
RUN go mod download

# Copy the go source
COPY pkg/ pkg/
COPY cmd/portworx-populator/ cmd/portworx-populator/

# Build portworx-populator binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o portworx-populator cmd/portworx-populator/portworx-populator.go

# Build fa_pxd_migration binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o fa_pxd_migration ./cmd/portworx-populator/fa-pxd-migration

# Runtime stage
FROM registry.access.redhat.com/ubi9/ubi-minimal:latest

# Install required tools (sg_xcopy for XCOPY operations, pxctl will be mounted from host)
RUN microdnf install -y \
    util-linux \
    sg3_utils \
    && microdnf clean all

WORKDIR /

# Copy binaries from builder
COPY --from=builder /workspace/portworx-populator /usr/local/bin/portworx-populator
COPY --from=builder /workspace/fa_pxd_migration /usr/local/bin/fa_pxd_migration

# Make binaries executable
RUN chmod +x /usr/local/bin/portworx-populator /usr/local/bin/fa_pxd_migration

# Note: No USER directive - this container requires root for nsenter, mdadm, pxctl, etc.

ENTRYPOINT ["/usr/local/bin/portworx-populator"]

