# API Key Swapper (keyswap)

Keep an AI app running when an API key hits its rate limit, a provider has an outage, or a model gets renamed.

keyswap sits between your code and an AI provider. You give it several API keys that you own. For each request it
picks a healthy key, and when something fails it responds in the right way for that kind of failure. It works as a
**Python library** or as a small **HTTP proxy**, so an app in any language can use it.

**Current version: [`rate_limiter_and_key_swapper_v2/`](rate_limiter_and_key_swapper_v2/README.md).** Start there.

## What it handles

| Failure | What keyswap does |
|---|---|
| `429` per-minute limit | Cools down only that key and retries on the next one. Uses `Retry-After` when the provider sends it, otherwise backs off exponentially with jitter. |
| `429` / `402` quota used up | Benches that key for a random 12-24 hours. |
| `401` or invalid key | Disables that key until you reinstate it. |
| `502` / `503` / `504` / `529` | Treats it as a provider outage. Backs off the provider and does not penalise any key. |
| A cooled-down key comes back | Sends one probe request first, then returns the key to full traffic. |
| A model name is retired | Falls back through the alias's model list and logs a warning. |
| A stream | Swaps keys before the first byte, so a `429` cannot break a stream halfway. |

## How it changed from v1 to v2

v1 was a single-file proxy that rotated keys in turn and benched any key that got a `429` for 12-24 hours. Using it
showed where it failed, and v2 was rebuilt around those failures.

```
v1: request -> next key in turn -> 429 -> bench key 12-24h -> retry
v2: request -> pick healthy key (probe a recovering one first)
                 -> 429 burst   -> cool down that key (Retry-After or backoff)
                 -> 429 quota   -> bench that key 12-24h
                 -> 401         -> disable that key until reinstated
                 -> 5xx         -> back off the provider, keys not blamed
                 -> stream      -> swap keys before the first byte
```

| Area | v1 | v2 |
|---|---|---|
| Key choice | Round robin | Health-aware: probe first, then least busy key |
| `429` | 12-24h bench every time | Short backoff for bursts, long bench only for quota |
| Provider outage (`5xx`) | Passed straight to the caller | Retried with backoff; keys not penalised |
| Revoked key (`401`) | Stayed in rotation | Disabled until reinstated |
| Retired model | Error to the caller | Alias fallback chain with a warning |
| Keys | Read from env at startup | Added or removed while running; loaded from env, `.env` or file |
| Restarts | Cool-downs lost | Optional health file (ids and times, never secrets) |
| Use | HTTP proxy only | Python library, with the proxy as an optional extra |
| Tests | 42 to 46 (v1 suite) | 77, using a fake provider (no network or keys) |

The full list of 12 problems and how each was fixed is in
[docs/v1-to-v2.md](rate_limiter_and_key_swapper_v2/docs/v1-to-v2.md).

## Repository layout

| Path | What it is |
|---|---|
| [`rate_limiter_and_key_swapper_v2/`](rate_limiter_and_key_swapper_v2/) | Current version: library, proxy, tests and docs |
| [`rate_limiter_and_key_swapper_v2/docs/v1-to-v2.md`](rate_limiter_and_key_swapper_v2/docs/v1-to-v2.md) | The 12 problems found in v1 and how v2 fixes them |
| [`rate_limiter_and_key_swapper_v1/`](rate_limiter_and_key_swapper_v1/) | The first prototype, kept as history. Single-file proxy with a fixed 12-24 hour bench on every `429`. Not maintained |

## Quick start

```bash
cd rate_limiter_and_key_swapper_v2
pip install ".[proxy]"
cp .env.example .env        # add your keys and a PROXY_AUTH_TOKEN
python -m keyswap           # listens on 127.0.0.1:8000
```

Library usage, configuration, error types, limitations and tests are all in the
[v2 README](rate_limiter_and_key_swapper_v2/README.md).

## Status

- v2 has 77 tests that use a fake provider, so they need no network or real keys. Run them with `pytest` from the
  v2 folder after `pip install -r requirements-dev.txt`.
- Add a CI badge or a pasted test result here once the suite runs on GitHub Actions, so the claim can be checked.
- The Anthropic and Gemini presets have not been checked against the live APIs yet. Reports are welcome.
- Run a single worker. Key state is held in one process.

## Use keys you own

Spreading traffic over keys that belong to you or your organisation is the intended use. Some providers restrict
creating several accounts to get around rate limits, so check the provider's terms first.

## License

MIT. See [LICENSE](LICENSE).
