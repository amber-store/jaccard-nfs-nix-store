# The jaccard-nfs-nix-store image: the sidecar, which serves the references
# of a jaccard-store server over NFS on loopback and mounts its own server
# on a directory the pod shares. It is configured through the JACCARD_*
# variables; see the README and deploy/pod.yaml.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/ ./cmd/jaccard-nfs-nix-store

FROM alpine:3.23
LABEL org.opencontainers.image.source=https://github.com/amber-store/jaccard-nfs-nix-store
LABEL org.opencontainers.image.description="jaccard-nfs-nix-store: a sidecar that serves jaccard-store references as a directory that fetches what is named"
LABEL org.opencontainers.image.licenses=LGPL-3.0-only
# The certificates are for the bucket, which the packs are fetched from
# over HTTPS. mountpoint, for the pod's startup probe, is BusyBox's.
RUN apk add --no-cache ca-certificates
COPY --from=build /out/jaccard-nfs-nix-store /usr/local/bin/
# The sidecar mounts, so it runs as root, and in a privileged container.
ENTRYPOINT ["/usr/local/bin/jaccard-nfs-nix-store"]
