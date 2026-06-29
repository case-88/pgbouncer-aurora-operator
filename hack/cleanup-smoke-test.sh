#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-pgbouncer-aurora}"
CR_NAME="${CR_NAME:-example-pg}"
DELETE_CRD="${DELETE_CRD:-false}"
DELETE_NAMESPACE="${DELETE_NAMESPACE:-false}"
CRD_NAME="pgbouncerauroras.pgbouncer-aurora.io"
MANAGED_SELECTOR="pgbouncer-aurora.io/managed-by=pgbouncer-aurora-operator"

namespace_exists() {
  kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1
}

crd_exists() {
  kubectl get crd "${CRD_NAME}" >/dev/null 2>&1
}

if namespace_exists; then
  if crd_exists; then
    kubectl -n "${NAMESPACE}" delete pgbounceraurora "${CR_NAME}" --ignore-not-found=true
    kubectl -n "${NAMESPACE}" wait --for=delete "pgbounceraurora/${CR_NAME}" --timeout=60s >/dev/null 2>&1 || true
  else
    echo "skip custom resource cleanup: CRD ${CRD_NAME} not found"
  fi

  kubectl -n "${NAMESPACE}" delete deployment,service,configmap \
    -l "${MANAGED_SELECTOR}" \
    --ignore-not-found=true

  kubectl -n "${NAMESPACE}" delete secret \
    pgbouncer-operator-db-auth \
    pgbouncer-monitor-auth \
    pgbouncer-userlist \
    --ignore-not-found=true

  kubectl -n "${NAMESPACE}" delete deployment pgbouncer-aurora-operator --ignore-not-found=true
  kubectl -n "${NAMESPACE}" delete rolebinding pgbouncer-aurora-operator --ignore-not-found=true
  kubectl -n "${NAMESPACE}" delete role pgbouncer-aurora-operator --ignore-not-found=true
  kubectl -n "${NAMESPACE}" delete serviceaccount pgbouncer-aurora-operator --ignore-not-found=true
else
  echo "skip namespaced cleanup: namespace ${NAMESPACE} not found"
fi

if [[ "${DELETE_CRD}" == "true" ]]; then
  kubectl delete crd "${CRD_NAME}" --ignore-not-found=true
fi

if [[ "${DELETE_NAMESPACE}" == "true" ]]; then
  kubectl delete namespace "${NAMESPACE}" --ignore-not-found=true
fi

echo "cleanup ok"
