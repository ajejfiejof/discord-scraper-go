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

## Style

- `go vet`, `gofmt`, `T440p` `64K` L2 awareness.
- Commit: `feat(writer): ...`, `fix(ratelimit): ...`, `chore(license): ...` (Conventional).

## License

By contributing you agree to license your contributions under `AGPLv3` (Copyright `Ashley Melanie Brooks`). No CLA needed — sole contributor history, but retro-AGPLv3 applies.

## Security

Never commit tokens — use `DISCORD_TOKEN` env. `.gitignore` covers `*.token`, `.env`, `output/`, `bin/`.
