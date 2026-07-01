# Contributing

This project publishes versioned releases. The `PgBouncerAurora` API is
`v1alpha1`.

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

Examples:

- `v0.1.0`
- `v0.1.1`
- `v0.2.0`
- `v0.2.0-alpha.1`

Rules:

- `v0.x` is unstable; breaking changes are allowed but must be documented.
- Do not move or reuse released Git tags.
- Do not push different images to an existing release image tag.
- Prefer explicit version tags in documentation and examples.
- Keep Git tag, GitHub Release, Quay image tag, and raw manifest URLs aligned.

## When To Cut A New Release

Cut a new release when an installable artifact changes:

- operator code
- CRD schema
- deployment manifests
- Quay image
- install flow based on versioned raw GitHub URLs

README wording-only changes usually do not need a new release.

## Release Checklist

1. Open a release PR and pass the repository ruleset requirements.
2. Merge the release PR to `main`.
3. From the merged `main` commit, run tests and vulnerability checks.
4. Create and push the release Git tag for that exact commit.
5. Build and push the multi-arch Quay image from the tagged commit.
6. Verify the image with `docker buildx imagetools inspect`.
7. Verify the raw CRD URL for the tag returns HTTP 200.
8. Create a GitHub Release for the tag, including the image tag/digest and raw
   CRD URL.

## Security

- Never commit real credentials, private keys, or production endpoints.
- Keep example secrets as placeholders only.
- Do not commit local `example/` manifests with real environment values.
- The status endpoint exposes topology metadata; production deployments should
  restrict access.
