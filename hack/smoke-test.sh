#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="${NAMESPACE:-pgbouncer-aurora}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-pgbouncer-aurora-operator:test}"
SECRET_FILE="${SECRET_FILE:-${ROOT_DIR}/deploy/secrets.yaml}"
CR_FILE="${CR_FILE:-${ROOT_DIR}/deploy/cr.yaml}"
SERVICE_ACCOUNT_ROLE_ARN="${SERVICE_ACCOUNT_ROLE_ARN:-}"
DRY_RUN="${DRY_RUN:-client}"
VALIDATE="${VALIDATE:-false}"
APPLY="${APPLY:-false}"
CRD_NAME="pgbouncerauroras.pgbouncer-aurora.io"
CRD_FILE="${ROOT_DIR}/deploy/crd.yaml"
SERVICE_ACCOUNT_FILE="${ROOT_DIR}/deploy/serviceaccount.yaml"
ROLE_FILE="${ROOT_DIR}/deploy/role.yaml"
ROLE_BINDING_FILE="${ROOT_DIR}/deploy/rolebinding.yaml"
OPERATOR_FILE="${ROOT_DIR}/deploy/operator.yaml"

if [[ "${APPLY}" != "true" ]]; then
  if [[ "${DRY_RUN}" == "client" ]]; then
    kubectl apply --dry-run=client --validate=false \
      -f "${CRD_FILE}" \
      -f "${SERVICE_ACCOUNT_FILE}" \
      -f "${ROLE_FILE}" \
      -f "${ROLE_BINDING_FILE}" \
      -f "${OPERATOR_FILE}" \
      >/dev/null
    kubectl -n "${NAMESPACE}" apply --dry-run=client --validate=false -f "${SECRET_FILE}" >/dev/null
    echo "client dry-run rendered raw manifests only; sample CR validation requires an installed CRD."
  else
    if [[ "${DRY_RUN}" == "server" ]] && ! kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; then
      echo "server dry-run requires an existing namespace because dry-run namespace creation is not persisted: ${NAMESPACE}" >&2
      echo "retry with NAMESPACE=<existing-namespace> or run APPLY=true after review." >&2
      exit 1
    fi
    kubectl apply --dry-run="${DRY_RUN}" --validate="${VALIDATE}" \
      -f "${CRD_FILE}" \
      -f "${SERVICE_ACCOUNT_FILE}" \
      -f "${ROLE_FILE}" \
      -f "${ROLE_BINDING_FILE}" \
      -f "${OPERATOR_FILE}"
    kubectl -n "${NAMESPACE}" apply --dry-run="${DRY_RUN}" --validate="${VALIDATE}" -f "${SECRET_FILE}"
    if kubectl get crd "${CRD_NAME}" >/dev/null 2>&1; then
      kubectl -n "${NAMESPACE}" apply --dry-run="${DRY_RUN}" --validate="${VALIDATE}" -f "${CR_FILE}"
    else
      echo "sample CR server dry-run skipped: CRD is not installed yet; operator manifests and Secrets were validated."
    fi
  fi
  echo "dry-run ok. set APPLY=true to apply resources."
  exit 0
fi

kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1 || kubectl create namespace "${NAMESPACE}"
kubectl apply \
  -f "${CRD_FILE}" \
  -f "${SERVICE_ACCOUNT_FILE}" \
  -f "${ROLE_FILE}" \
  -f "${ROLE_BINDING_FILE}" \
  -f "${OPERATOR_FILE}"
kubectl wait --for=condition=Established "crd/${CRD_NAME}" --timeout=60s
if [[ -n "${SERVICE_ACCOUNT_ROLE_ARN}" ]]; then
  kubectl -n "${NAMESPACE}" annotate serviceaccount pgbouncer-aurora-operator \
    "eks.amazonaws.com/role-arn=${SERVICE_ACCOUNT_ROLE_ARN}" \
    --overwrite
fi
kubectl -n "${NAMESPACE}" set image deploy/pgbouncer-aurora-operator "manager=${OPERATOR_IMAGE}"
kubectl -n "${NAMESPACE}" rollout status deploy/pgbouncer-aurora-operator --timeout=120s
kubectl -n "${NAMESPACE}" apply -f "${SECRET_FILE}"
kubectl -n "${NAMESPACE}" apply -f "${CR_FILE}"

kubectl -n "${NAMESPACE}" get pgba
kubectl -n "${NAMESPACE}" get deploy,svc,pod -l pgbouncer-aurora.io/managed-by=pgbouncer-aurora-operator
