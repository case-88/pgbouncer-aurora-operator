#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

DOCKER="${DOCKER:-false}"
IMAGE_TAG="${IMAGE_TAG:-pgbouncer-aurora-operator:audit}"

echo "==> go test"
go test -count=1 ./...

echo "==> go build"
go build ./...

echo "==> go vet"
go vet ./...

echo "==> manifest client dry-run"
kubectl apply --dry-run=client --validate=false \
  -f deploy/crd.yaml \
  -f deploy/serviceaccount.yaml \
  -f deploy/role.yaml \
  -f deploy/rolebinding.yaml \
  -f deploy/operator.yaml \
  >/tmp/pgbouncer-aurora-operator-manifests.yaml

echo "==> smoke-test syntax"
bash -n hack/smoke-test.sh
bash -n hack/cleanup-smoke-test.sh

echo "==> smoke-test client dry-run"
./hack/smoke-test.sh

if [[ "${DOCKER}" == "true" ]]; then
  echo "==> docker buildx"
  docker buildx build --platform linux/amd64 -t "${IMAGE_TAG}" --load .
fi

echo "check ok"
