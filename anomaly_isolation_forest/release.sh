#!/usr/bin/env bash
set -euo pipefail

# Log in (make sure to secure your credentials in CI secrets)
docker login quay.io -u 'jpaulraj_se_jboss' -p 'Redhat2024$'

# Define your image base and version
IMAGE=quay.io/zagaos/anomaly-isolation-forest
VERSION=1.1

# Default: versioned tag; also push latest
TAG_VERSIONED=${IMAGE}:${VERSION}
TAG_LATEST=${IMAGE}:latest

# Ensure buildx builder exists
docker buildx ls | grep multiarch || docker buildx create --name multiarch --use

# Build & push both tags for arm64 and amd64
docker buildx build --push \
  --platform linux/arm64,linux/amd64 \
  --tag "$TAG_VERSIONED" \
  --tag "$TAG_LATEST" \
  --progress=plain \
  .

    .