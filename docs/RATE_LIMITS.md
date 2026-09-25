# Rate Limits — Ban-Safe

Discord `50 RPS` global, `~5/s` per `channel_id` bucket. This scraper stays at `56-72%` of that.

## Strategy (AATB — arXiv:2510.04516)

`internal/ratelimit/limiter.go` implements Augmented Adaptive Token Bucket:

- **Continuous refill:** `tokens += elapsed * targetRPS`, `maxBurst 15`, `lastRefill` — no artificial `nextAllowed` delay when burst available.
- **Major-param isolation:** `pathBucketKey` returns `/channels/:id/messages` for channel routes, so `channel A` 429 doesn’t block `channel B`.
- **Headers:** `X-RateLimit-Remaining/Limit/Reset-After` → `UpdateFromHeaders`, `RetryAfter` JSON + `Retry-After` header → `Register429` with `10-30ms` jitter + `10%` safety.
- **Global 429:** `globalUntil` + `nextAllowed` snap.

## Defaults (T440p)

- `target-rps 36`, `concurrency 8` (1/thread `i7-4712MQ`), `maxBurst 15`. `32-36` is ban-safe, `45` is cap.
- `8×36` → `~28` sustained (jitter + per-bucket throttling), `0` global `429` cascades in live `1M+` tests.

## Tuning

- Start `12/32` on desktops, `8/36` on `8GB`. If `verbose` shows `skip channel (no access, raw) 403` often, lower to `8/28`.
- `429` handling: `180-300ms` safety buffer, exponential `300ms*2^attempt` for `5xx` up to `5s`, `12` retries.
- `403 Missing Access` (private `VC`, `rules-for-new-members`) is non-fatal — `isPermissionError` checks `403/50001/50013`, skips channel, continues guild.

## Scaling with Multiple Tokens

- `N` tokens → `N*50 RPS` global, `N*5/s` per channel if sharded `channel_id % N` (needs `[]*Client` pool + `hash` in `extractor.go`). `IP` limit `~1000 RPS` per `/24`, needs `3-5` residential proxies for `>3` tokens. Per-channel cursor is serial, so sharding a single `example-channel` across tokens needs handoff.

## Monitoring

```bash
./bin/scraper --guild 123... --concurrency 8 --target-rps 36 --verbose 2>&1 | grep -E "429|429|skip|Remaining"
```

If every `Wait` sleeps, lower `target-rps` or `concurrency`.
