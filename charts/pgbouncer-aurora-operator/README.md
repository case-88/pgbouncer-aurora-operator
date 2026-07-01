# pgbouncer-aurora-operator Helm chart

Helm chart for installing the `pgbouncer-aurora-operator`.

The chart installs the `PgBouncerAurora` CRD and the operator runtime. It does
not create `PgBouncerAurora` custom resources or DB credential Secrets. Manage
those as separate manifests per release/environment.

## Scope

The chart installs:

- `PgBouncerAurora` CRD under `crds/`
- ServiceAccount
- namespace-scoped Role and RoleBinding
- operator Deployment
- metrics Service
- status NetworkPolicy

The chart intentionally does not provide cluster-wide watch mode. The operator
watches the Helm release namespace through a namespace-scoped Role.

## Install

Install from the Quay OCI registry:

```bash
helm install pgbouncer-aurora-operator oci://quay.io/case-88/charts/pgbouncer-aurora-operator \
  --version 0.1.0 --namespace pgbouncer-aurora --create-namespace
```

Then apply environment-specific Secrets and `PgBouncerAurora` CR manifests
separately.

## Upgrade

```bash
helm upgrade pgbouncer-aurora-operator oci://quay.io/case-88/charts/pgbouncer-aurora-operator \
  --version 0.1.0 --namespace pgbouncer-aurora
```

Reference: if you use Helm 3.17.0 or later, use `--take-ownership` to take
ownership of CRDs that were previously installed through a chart `crds/`
directory:

```bash
helm upgrade pgbouncer-aurora-operator oci://quay.io/case-88/charts/pgbouncer-aurora-operator \
  --version 0.1.0 --namespace pgbouncer-aurora --take-ownership
```

For local development, replace the OCI reference with
`charts/pgbouncer-aurora-operator`.

## Publish

Package and push the chart to Quay OCI:

```bash
helm package charts/pgbouncer-aurora-operator --destination dist/charts
helm push dist/charts/pgbouncer-aurora-operator-0.1.0.tgz oci://quay.io/case-88/charts
```

## Values

| Value | Default | Description |
|---|---|---|
| `image.repository` | `quay.io/case-88/pgbouncer-aurora-operator` | Operator image repository. |
| `image.tag` | `v0.1.0` | Operator image tag. |
| `serviceAccount.annotations` | `{}` | ServiceAccount annotations, for example IRSA. |
| `watch.names` | `"*"` | `*` or comma-separated `PgBouncerAurora` names to watch in the release namespace. |
| `aws.region` | `ap-northeast-2` | AWS region for RDS metadata lookup. |
| `aws.apiQPS` | `1` | Defensive AWS API limiter QPS. |
| `aws.apiBurst` | `1` | Defensive AWS API limiter burst. |
| `controller.maxConcurrentReconciles` | `64` | Maximum concurrent reconciles across CRs. |
| `controller.reconcileMinInterval` | `1s` | Per-CR heavy reconcile minimum interval. |
| `controller.rdsMetadataRefreshInterval` | `1m` | Shared RDS metadata refresh interval. |
| `controller.workersPerCR` | `10` | Maximum concurrent backend monitor probes per CR. |
| `controller.k8sApiTimeout` | `10s` | Timeout for short-lived Kubernetes API requests. |
| `status.refreshMinInterval` | `5s` | Minimum `/status` snapshot refresh interval. |
| `status.recentWindow` | `1m` | Recent-change highlight window for `/status`. |
| `extraArgs` | `[]` | Additional manager args. |
| `extraEnv` | `[]` | Additional manager env entries. |
| `extraVolumeMounts` | `[]` | Additional manager container volume mounts. |
| `extraVolumes` | `[]` | Additional pod volumes. |

## Validation

```bash
helm lint charts/pgbouncer-aurora-operator
helm template pgbouncer-aurora-operator charts/pgbouncer-aurora-operator
helm template pgbouncer-aurora-operator charts/pgbouncer-aurora-operator \
  --namespace pgbouncer-aurora \
  | kubectl apply --dry-run=client --validate=false -f -
helm install pgbouncer-aurora-operator charts/pgbouncer-aurora-operator \
  --namespace pgbouncer-aurora \
  --create-namespace \
  --dry-run=server
```
