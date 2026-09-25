# Contributing

## Dev

```bash
go vet ./...
go test ./... -count=1          # ratelimit/writer/snowflake
go test ./internal/writer -bench=. -benchmem  # zero-copy bench
make build  # bin/scraper 11M
./bin/scraper --help
```

`golangci-lint` if available: `make lint`.

## Commit Policy — Conventional Commits (GitHub Standard)

We follow **Conventional Commits v1.0.0** (https://www.conventionalcommits.org) — the GitHub standard for automated changelogs, semantic versioning, and `git log` readability. This is enforced via `commitlint` in CI and `git log --oneline` expectations.

**Format:** `type(scope): subject` — lowercase `type`, optional `scope`, imperative `subject` (no period).

**Types (common on GitHub):**
- `feat`: new feature (minor bump)
- `fix`: bug fix (patch)
- `docs`: documentation only
- `chore`: tooling, deps, license
- `refactor`: restructuring, no feat/fix
- `perf`: performance improvement
- `test`: tests
- `ci`: CI/workflow
- `revert`: reverts a previous commit

**Scopes (our repo):** `writer`, `ratelimit`, `discord`, `extractor`, `config`, `checkpoint`, `scraper`, `docs`, `license`

**Examples:**
```
feat(writer): zero-copy stream ingestion + Sparser filtering, L2 cache
fix(ratelimit): AATB continuous token bucket
docs: add ARCHITECTURE/USAGE/RATE_LIMITS
chore(license): switch MIT -> AGPLv3 (sole contributor)
feat(parity): enrich all visible user profiles
```

**Rules:**
- Subject `<=72` chars, imperative, no `WIP`, no `fixup!`
- Body wraps at `72`, explains *why* not *what* (link `arXiv`, `PVLDB`, `issue #`)
- `BREAKING CHANGE:` footer for major (or `feat!:`)
- One logical change per commit (atomic) — `git add -p`, not `git add .` dumps
- Sign-off: `git commit -s` (`Signed-off-by`) appreciated but not required

**Enforcement:** `commitlint` (`commitlint.config.js` extends `@commitlint/config-conventional`) runs on PRs via `.github/workflows/commitlint.yml`. Squash-merge uses the PR title as commit message — it must be Conventional too.

**Branching:** `main` is protected, `feat/*` or `fix/*` branches, PR required, `go vet` + `go test` must pass, `1` reviewer.

**History:** Linear, no merge commits on `main` (`Rebase and merge` or `Squash`). Use `git pull --rebase`, `git commit --amend` for fixups before push.

## Code of Conduct

This project adopts the **Contributor Covenant v2.1** (`.github/CODE_OF_CONDUCT.md`). Report issues to `ashley@pratherbytecraft.com` or private GitHub Issue. See `CODE_OF_CONDUCT.md` for enforcement ladder.

## License

By contributing you agree to license your contributions under `AGPLv3` (Copyright `Ashley Melanie Brooks`). No CLA needed — sole contributor history, but retro-AGPLv3 applies.

## Security

Never commit tokens — use `DISCORD_TOKEN` env. `.gitignore` covers `*.token`, `.env`, `output/`, `bin/`.
