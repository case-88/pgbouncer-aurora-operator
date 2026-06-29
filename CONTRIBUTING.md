# Contributing

This project is in public alpha. The `PgBouncerAurora` API is `v1alpha1`, and
breaking changes may happen before `v0.1.0`.

## Pull Requests

- Keep changes focused.
- Use short branches such as `feat/...`, `fix/...`, `docs/...`, or `chore/...`.
- Explain user-visible behavior, CRD, or manifest changes in the PR body.
- Add or update tests when controller, planner, discovery, monitor, or rendering
  behavior changes.
- Update README examples or configuration tables when user-facing options
  change.

## Local Checks

Run the relevant checks before opening a PR:

```bash
go test -count=1 ./...
go vet ./...
go run golang.org/x/vuln/cmd/govulncheck@latest ./...
```

For Docker changes:

```bash
docker buildx build --platform linux/amd64 -t pgbouncer-aurora-operator:dev --load .
```

The helper script is also available:

```bash
./hack/check.sh
```

## Versioning

Release tags use:

```text
vMAJOR.MINOR.PATCH[-PRERELEASE]
```

Current pre-release flow is alpha-only:

- `v0.1.0-alpha`
- `v0.1.0-alpha.1`
- `v0.1.0-alpha.2`
- `v0.1.0`

Rules:

- `v0.x` is unstable; breaking changes are allowed but must be documented.
- Do not move or reuse released Git tags.
- Do not push different images to an existing release image tag.
- Do not publish `latest` during alpha.
- Keep Git tag, GitHub Release, Quay image tag, and raw manifest URLs aligned.

## When To Cut A New Alpha

Cut a new alpha when an installable artifact changes:

- operator code
- CRD schema
- deployment manifests
- Quay image
- install flow based on versioned raw GitHub URLs

README wording-only changes usually do not need a new release.

## Release Checklist

1. Merge the intended release commit to `main`.
2. Run tests and vulnerability checks.
3. Build and push the multi-arch Quay image for the exact version tag.
4. Verify the image with `docker buildx imagetools inspect`.
5. Verify the raw CRD URL returns HTTP 200.
6. Create a GitHub pre-release for the tag.

## Security

- Never commit real credentials, private keys, or production endpoints.
- Keep example secrets as placeholders only.
- Do not commit local `example/` manifests with real environment values.
- The status endpoint exposes topology metadata; production deployments should
  restrict access.
