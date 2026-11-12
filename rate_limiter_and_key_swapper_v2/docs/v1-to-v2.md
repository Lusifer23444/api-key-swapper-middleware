# From v1 to v2

v1 (kept in [`rate_limiter_and_key_swapper_v1/`](../../rate_limiter_and_key_swapper_v1/rate_limiter_and_key_swapper.py)) was a single-file proxy that rotated API keys round
robin and benched a key for a fixed 12-24 hours whenever the provider returned `429`. It worked, and building it
showed where it fell short. v2 fixes those points.

## Why this project exists

While building an app on top of AI provider APIs, three things kept breaking it: rate limits (`429`), provider
unavailability (`503`), and a provider retiring the model name the code was calling (a Gemini "flash" model was
replaced by a newer version, and every call to the old name failed). v1 handled only the first one, and crudely.

## Problems in v1, and what v2 does

| # | v1 behaviour | Why it hurt | v2 |
|---|---|---|---|
| 1 | Every `429` benched the key for 12-24 h, even a one-minute burst limit | A short spike cost a key for a whole day | Honours `Retry-After`, otherwise exponential backoff with jitter. The long bench is only for real quota exhaustion. |
| 2 | `5xx` answers were passed straight back to the caller | No retry; a brief provider outage became an app outage | `502/503/504/529` back off the provider and retry (up to 2 times), without blaming any key. |
| 3 | The proxy's own "all keys exhausted" `503` looked like a provider `503` | Callers couldn't tell whose problem it was | Distinct `error.type` values (`all_keys_exhausted`, `provider_unavailable`, ...). |
| 4 | A `401` (revoked key) was relayed and the key stayed in rotation | A dead key kept being picked and failing | A `401` or "invalid key" **disables** the key until `reinstate`. |
| 5 | A key rejoined full traffic the moment its timer ended | A recovering key could be hit by everything at once | One probe request first (the "probation layer"), then full traffic. |
| 6 | Round robin ignored key health and load | It couldn't say what state each key was in | State per key (active, cooling, probing, disabled); picks the least busy active key; `status()` shows it all. |
| 7 | A retired or renamed model returned an error to the caller | The app went down until someone edited code | Model aliases with a fallback chain and a warning in the log. |
| 8 | Keys came only from `API_KEYS` / `.env`, read at startup | No adding or removing keys while running | `add_key`, `remove_key`, `POST /proxy/keys`, and optional loaders for env vars, `.env` and files. `.env` is no longer required. |
| 9 | One fixed upstream URL set through environment variables | Hard to reuse in code | A `Provider` object (with presets); usable as a library or through the proxy. |
| 10 | Cooldowns were lost on restart | Benched keys were retried immediately | Optional JSON health file with ids and times only (no secrets). |
| 11 | Settings and the key pool were created when the module was imported | Awkward to test | Objects are built explicitly, `create_app(gateway)` takes one, and the suite runs on a mocked provider. |
| 12 | A `402/403` body containing words like "quota" or "billing" benched the key | A false positive could bench a good key | Responses are sorted into burst, quota, bad key, model gone and provider error. Still heuristic, but covered by tests. |

## Kept from v1

Streaming with key swapping before the first byte, a fail-closed admin surface, a client-header allowlist, stripping of
response headers that reveal the provider account, request and response size limits, an overall request deadline,
and no redirects.

## Changed or dropped on purpose

- v1's `POST /proxy/keys/reinject` and `GET /proxy/keys/benched` became `GET /proxy/keys` and
  `POST /proxy/keys/{key_id}/reinstate`. There is no "reinstate all" call yet.
- The proxy is now an optional part (`pip install ".[proxy]"`) on top of a library.

## Still not solved (honest list)

One provider per gateway, state kept in a single process, no token-per-minute budgeting, and error detection that
depends on provider wording. See "Limitations" in the README.
