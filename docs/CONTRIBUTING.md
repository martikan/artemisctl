# Contributing to artemisctl

Thanks for your interest in improving `artemisctl`. This guide covers how to
report problems, propose changes, and get a pull request merged.

By participating you agree to keep the tone respectful and constructive — assume
good faith, keep discussions technical, and help newcomers.

## Ways to contribute

- **Report a bug** — open an issue with a clear reproduction.
- **Request a feature** — open an issue describing the use case before writing
  code, so we can agree on scope and design first.
- **Improve docs** — fixes to the README, this guide, or the design notes under
  `docs/design/` are always welcome.
- **Send a pull request** — bug fixes and features (see the workflow below).

If you are unsure whether a change is wanted, open an issue first. A small
discussion up front is cheaper than a rejected pull request.

## Reporting issues

Search the existing issues first to avoid duplicates. A good bug report has:

- **What you did** — the exact `artemisctl` command and flags.
- **What you expected** vs **what happened** — include the full error output.
- **Environment** — `artemisctl` version (or commit), OS/arch, and the Artemis
  broker version you targeted.
- **A minimal reproduction** — the smallest setup that still shows the problem.

For feature requests, describe the problem you are trying to solve, not just the
solution you have in mind — it helps us find the best fit for the tool.

### Security issues

Please do **not** open a public issue for a security vulnerability. Report it
privately to the maintainer (via the email on the GitHub profile, or a GitHub
security advisory) so a fix can ship before disclosure.

## Development setup

You need [Go](https://go.dev/dl/) (the version pinned in [`go.mod`](../go.mod),
currently 1.25) and [Docker](https://docs.docker.com/get-docker/) running —
the integration tests boot a real Artemis broker via
[Testcontainers](https://testcontainers.com/), so Docker must be available.

```bash
git clone https://github.com/martikan/artemisctl.git
cd artemisctl

make build      # compile to bin/artemisctl
make help       # list every make target
```

## Coding standards

- **Format** with `gofmt` — CI rejects any unformatted file. `make test` runs
  `gofmt -s -w .` for you; you can also run `gofmt -l .` to list offenders.
- **Vet** clean — `go vet ./...` must pass (also part of `make test` and CI).
- **Match the surrounding code** — naming, comment density, and idiom. Prefer
  small, focused functions and explain *why* in comments, not *what*.
- **Keep changes focused** — one logical change per pull request. Unrelated
  cleanups belong in their own PR.

## Testing

```bash
make test         # gofmt + go vet + the full suite (boots Artemis containers)
make coverage     # same suite with -race + coverage, writes coverage.html
```

- **Add tests for every change.** Bug fixes get a regression test; features get
  unit and/or integration coverage.
- **Unit tests** must not need a broker — they run under `go test -short`.
- **Integration tests** may boot a container; guard them with
  `if testing.Short() { t.Skip(...) }` like the existing ones so `-short` stays
  broker-free.
- **Coverage gate:** CI enforces a project-wide **85%** statement coverage
  threshold (see [`.github/workflows/quality-gate.yml`](../.github/workflows/quality-gate.yml)).
  A pull request that drops coverage below the bar will fail. The workflow posts
  a coverage report as a PR comment.

## Commit messages

- Use the **imperative mood** ("Add drain retry", not "Added" / "Adds").
- Keep the subject line short (≈50 chars) and lowercase-free of a trailing dot.
- [Conventional Commits](https://www.conventionalcommits.org/) prefixes
  (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`) are encouraged —
  they make history and release notes easier to read.
- Explain the **why** in the body when it isn't obvious from the diff.

## Pull request workflow

1. **Fork** the repo and create a topic branch off `main`
   (`git checkout -b fix/browse-offset`).
2. **Make your change** with tests, keeping the branch focused.
3. **Run the gate locally** before pushing:

   ```bash
   gofmt -l .        # must print nothing
   go vet ./...
   make coverage     # tests pass, coverage stays >= 85%
   ```

4. **Open the pull request** against `main`. Describe what changed and why, and
   link any related issue (`Closes #123`).
5. **Green CI is required.** The `quality-gate` workflow runs lint, the full test
   suite, and the coverage check on every pull request.
6. **Address review feedback** by pushing follow-up commits. A maintainer merges
   once CI is green and the review is resolved.

Keep pull requests small where you can — they are faster to review and safer to
merge.

## Releases

Releases are cut by maintainers by pushing a `v*` tag. That triggers the
[`ci-cd`](../.github/workflows/ci-cd.yml) workflow, which produces
[SLSA3](https://slsa.dev) provenance-signed binaries on the GitHub Release and
pushes a container image to `ghcr.io/martikan/artemisctl`. Contributors do not
need to touch the release process.

---

Questions that don't fit an issue? Open a
[discussion](https://github.com/martikan/artemisctl/discussions) or ask in the
issue tracker. Thanks for contributing!
