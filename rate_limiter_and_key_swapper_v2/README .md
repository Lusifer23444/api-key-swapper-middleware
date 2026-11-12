# keyswap

**Keep your AI app running when an API key hits its limit, a provider has a bad day, or a model gets renamed.**

keyswap sits between your code and an AI provider (OpenAI, Anthropic, Gemini's OpenAI-compatible endpoint, or any
OpenAI-style API). You give it several API keys that you own. For every request it picks a healthy key, and when
something goes wrong it reacts the right way instead of just failing.

It exists because three things kept breaking a real app: `429` rate limits, `503` provider outages, and a provider
retiring the model name the code was calling. keyswap treats each of those as its own problem.

- Use it as a **Python library** (`pip install`, call `gw.chat(...)`), or
- run it as a small **HTTP proxy** so an app in any language can use it.

> Status: version 2.0.0. Tested with mocked providers and a local fake server (77 tests, no network or real keys
> needed). The presets for Anthropic and Gemini have not been verified against the live APIs, so reports are welcome.

## What it does

| What happens | What keyswap does |
|---|---|
| `429` (per-minute rate limit) | Cools down **only that key**, then retries on the next key. Uses the provider's `Retry-After` when sent, otherwise exponential backoff with jitter. |
| `429`/`402` that means the quota is used up | Benches that key for 12-24 hours. |
| `401` or "invalid API key" | **Disables** that key until you call `reinstate`. A dead key is never picked again. |
| `502` / `503` / `504` / `529` | Treats it as the **provider's** problem: backs off the whole provider (with jitter), retries up to 2 times, and **never blames your keys**. |
| A cooled-down key becomes eligible again | Sends **one** probe request first, and only then returns it to full traffic (no flood). |
| The provider says a model no longer exists | If you used an alias, tries the next model in its fallback chain and logs a loud warning. |
| Every key is cooling down | Raises `AllKeysUnavailable` with the number of seconds until the soonest key returns (the proxy answers `503` + `Retry-After`). |
| A stream is requested | Swaps keys **before the first byte**, so a `429` can't break a stream halfway. |

Other requests (bad input, a normal `500`, ...) are returned to you unchanged, because rotating keys wouldn't help.

## Install

From the repository root:

```bash
pip install .            # library only (needs httpx)
pip install ".[proxy]"   # library + the HTTP proxy (adds fastapi and uvicorn)
```

or `pip install -r requirements.txt`. Python 3.9 or newer is declared; the test suite has been run on 3.11, 3.12
and 3.13.

## Quick start: library

```python
import asyncio
from keyswap import AllKeysUnavailable, Gateway, ModelMap, Provider, ProviderUnavailable


async def main():
    models = ModelMap({"fast": ["gpt-4o-mini", "gpt-4.1-mini"]})   # first name that still exists wins
    async with Gateway(Provider.openai(), models=models) as gw:
        gw.add_keys_from_env("API_KEYS")      # or gw.add_key("sk-...") from your own secret store
        try:
            reply = await gw.chat({"model": "fast", "messages": [{"role": "user", "content": "Say hi"}]})
            print(reply.json()["choices"][0]["message"]["content"], "| key:", reply.key_id)
        except AllKeysUnavailable as exc:
            print("Every key is cooling down. Retry in", exc.retry_after, "seconds")
        except ProviderUnavailable:
            print("The provider is having problems. Your keys are fine.")
        print(gw.status())                    # key ids and states, never the secrets


asyncio.run(main())
```

Streaming (include `"stream": True` in your payload):

```python
async with gw.stream({"model": "fast", "stream": True, "messages": [...]}) as s:
    async for chunk in s.aiter_bytes():
        print(chunk.decode(), end="")
    # s.failed is True if the provider's connection dropped midway; s.truncated if max_stream_s cut it off
```

Errors you may want to catch (all inherit from `KeySwapError`):

| Error | Meaning |
|---|---|
| `AllKeysUnavailable` | No key can serve right now. `.retry_after` is seconds until the soonest one, or `None` if none can recover on its own (empty pool, or all disabled). |
| `ProviderUnavailable` | The provider kept failing after the retries. No key was penalised. |
| `ModelUnavailable` | Every model in the alias's fallback chain was rejected. |
| `DeadlineExceeded` | The overall request deadline (default 120 s) ran out. |
| `GatewayBusy` | Too many requests in flight locally (connection pool full). |
| `ResponseTooLarge` | The provider's answer exceeded `max_response_bytes`. |

## Giving it keys (`.env` is optional)

You decide where keys come from. Nothing requires a `.env` file.

| Way | Code |
|---|---|
| From your own code or secret manager | `gw.add_key("sk-...")` / `gw.add_keys([...])` |
| Environment variable, or a `.env` file | `gw.add_keys_from_env("API_KEYS")` reads real environment variables and, if present, a `.env` file. A real variable wins over the same name in `.env`. |
| A text file (one key per line) | `gw.add_keys_from_file("keys.txt")` |
| While running, over HTTP | `POST /proxy/keys` (proxy only) |

Both styles work in env/`.env`: `API_KEYS=key1,key2,key3` and `API_KEYS_1=key1`, `API_KEYS_2=key2`. Duplicate keys are
ignored. The `.env` reader is built in, so beginners need no extra package.

Each key gets an id like `key-015f7e6b` (a fingerprint of the secret). Logs, status output and errors only ever show
ids. Secrets are never printed or written to disk.

## Model aliases (the "provider renamed my model" fix)

Ask for an alias such as `"fast"` or `"smart"` instead of a raw model name. Each alias maps to an ordered list of
real models. If the provider reports a model as gone, keyswap skips it for an hour, uses the next one, and logs a
warning so you update your map. Names that aren't aliases pass through unchanged.

```python
models = ModelMap({"fast": ["new-flash", "old-flash"]})
models.set_alias("smart", ["new-pro", "old-pro"])     # can be changed at runtime
```

This contains a rename or retirement. It cannot predict a change in a provider's request format.

## Remembering key health across restarts

```python
from keyswap import JsonStateStore
gw = Gateway(Provider.openai(), store=JsonStateStore("key_health.json"))
```

keyswap saves only the key id, state, cooldown end time and failure count. **No secrets are written.** Your app
registers its keys at startup, and the saved health is applied to them by id. Without a store, a restart forgets
cooldowns and a benched key is simply re-benched on its next `429`.

## Quick start: HTTP proxy

```bash
pip install ".[proxy]"
cp .env.example .env            # then edit it: add your keys and a PROXY_AUTH_TOKEN
python -m keyswap               # listens on 127.0.0.1:8000
```

Generate a token with `python -c "import secrets; print(secrets.token_urlsafe(32))"`.

```bash
curl http://127.0.0.1:8000/proxy/v1/chat \
  -H "Authorization: Bearer $PROXY_AUTH_TOKEN" -H "Content-Type: application/json" \
  -d '{"model": "fast", "messages": [{"role": "user", "content": "Say hi"}]}'
```

The body is the provider's own JSON, and `"stream": true` works. The proxy refuses to start without
`PROXY_AUTH_TOKEN` unless you set `ALLOW_UNAUTHENTICATED=1` (local development only).

| Endpoint | Purpose |
|---|---|
| `POST /proxy/v1/chat` | The proxied call |
| `GET /proxy/health` | Counts only; per-key states too when the admin token is sent |
| `GET /proxy/keys` | Key ids and states (admin) |
| `POST /proxy/keys` | Add a key at runtime: `{"key": "...", "key_id": "optional"}` (admin) |
| `DELETE /proxy/keys/{key_id}` | Remove a key; in-flight requests finish first (admin) |
| `POST /proxy/keys/{key_id}/reinstate` | Force a cooling or disabled key back to active (admin) |

Admin endpoints need the same bearer token, and stay **off** (403) when no token is configured.

Errors made by the proxy itself have an `error.type` so you can tell them apart from the provider's:

| `error.type` | Status | Meaning |
|---|---|---|
| `all_keys_exhausted` | 503 + `Retry-After` | Every key is cooling down |
| `no_usable_keys` | 503 | No key is registered, or all are disabled |
| `provider_unavailable` | 503 + `Retry-After` | The provider kept failing; keys not blamed |
| `proxy_busy` | 503 | Local connection pool is full |
| `model_unavailable` | 502 | Every model in the alias chain was rejected |
| `deadline_exceeded` | 504 | The request deadline ran out |
| `upstream_response_too_large` | 502 | The answer exceeded the size limit |
| `unauthorized` / `payload_too_large` / `bad_request` | 401 / 413 / 400 | Problem with your request |

### Proxy settings (environment variables or `.env`)

| Variable | Default | Meaning |
|---|---|---|
| `API_KEYS` | none | Comma-separated keys (or `API_KEYS_1`, `API_KEYS_2`, ...). May be empty; add keys later over HTTP. |
| `PROXY_AUTH_TOKEN` | required | Bearer token for clients and admin endpoints |
| `KEYSWAP_PROVIDER` | `openai` | `openai`, `anthropic`, `gemini` or `custom` (needs `UPSTREAM_BASE_URL`) |
| `UPSTREAM_BASE_URL`, `UPSTREAM_CHAT_PATH` | from preset | Override where requests go. Plain `http://` is refused unless local or `ALLOW_INSECURE_UPSTREAM=1` |
| `UPSTREAM_AUTH_HEADER`, `UPSTREAM_AUTH_SCHEME` | `Authorization`, `Bearer` | How the key is sent (e.g. `x-api-key` and an empty scheme) |
| `FORWARD_HEADERS` | from preset | Client headers allowed through to the provider (everything else is dropped) |
| `MODEL_ALIASES` | none | JSON, e.g. `{"fast": ["model-a", "model-b"]}` |
| `STATE_FILE` | none | Path of the key-health file (see above) |
| `REQUEST_DEADLINE_SECONDS` | 120 | Total time for one request, all retries included |
| `UPSTREAM_TIMEOUT_SECONDS` | 60 | Time allowed for one attempt |
| `MAX_STREAM_SECONDS` | 900 | Ceiling for one streamed answer |
| `MAX_REQUEST_BODY_BYTES` / `MAX_RESPONSE_BYTES` | 10 MiB / 32 MiB | Size limits |
| `MAX_CONNECTIONS`, `POOL_TIMEOUT_SECONDS` | 200, 5 | Connection pool size, and how long to wait for a free one |
| `HOST`, `PORT` | `127.0.0.1`, `8000` | Where to listen |
| `DISABLE_DOTENV` | off | Set `1` to ignore any `.env` file |

## How it works

```
ACTIVE --429 or quota--> COOLING --cooldown ends--> PROBING --probe succeeds--> ACTIVE
                                                       |--probe gets 429--> COOLING (longer wait)
any state --401 / invalid key--> DISABLED  (until you reinstate it)
```

- **Choosing a key:** a key whose cooldown has ended gets one probe request first. Otherwise the active key with
  the fewest requests in flight wins, ties going to the one used longest ago.
- **429 cooldown:** `Retry-After` if the provider sent it (capped at 1 hour). Otherwise 15 s, doubling with each
  consecutive failure up to 15 minutes, randomised between half and full of that value so keys don't recover in
  lockstep. A success resets the count.
- **Quota exhausted:** a random 12-24 hours.
- **Provider trouble (5xx, network errors):** a separate, shared backoff (about 1 s doubling up to 30 s, with
  jitter) and up to 2 retries. It never touches key health.
- Several requests reporting the same `429` at once count as one failure.
- All defaults live in `PoolConfig` and `GatewayConfig` and can be changed when you build the objects.

## Limitations (read before relying on it)

- **One provider per gateway.** All keys in a pool must belong to the same provider. Failing over from one provider
  to another is not built in.
- **State lives in one process.** Run a single worker. Multiple workers or machines each keep their own view
  (a shared store such as Redis would be needed, and is not included).
- **Request counting only.** It reacts to the provider's `429`s; it does not track tokens-per-minute budgets ahead
  of time.
- **Error detection is heuristic.** Telling a per-minute `429` from a quota `429` relies on the provider's wording
  (see `keyswap/classify.py`). Unusual wording falls back to the short cooldown.
- **A stream that breaks midway can only be cut short.** Keys are swapped before the first byte, never after.
- **Keys added over HTTP are held in memory only.** After a restart, add them again or load them from your source.
- **Not verified against live providers' newest API changes.** A new, incompatible request format still needs a human
  fix; aliases only handle renames.

## Use keys you own

Spreading traffic over keys that belong to you or your organisation (for example one per project or environment) and
surviving outages is the intended use. Some providers restrict creating multiple accounts or keys to get around rate
limits, so check your provider's terms.

## Security notes

- **Never commit `.env`.** It is in `.gitignore`; only `.env.example` (placeholders) belongs in the repo.
- Upstream credentials never come from the client. Client headers are allowlisted, so your proxy token and cookies
  can't leak to the provider, and headers that reveal the provider account are removed from responses.
- Redirects are never followed (a redirect would replay your key to another host).
- If you expose the proxy beyond your machine, put it behind HTTPS.

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

The tests use a fake provider, so they need no network and no real keys. They cover the 429, 401, 503 and
model-retired cases, probing, concurrent requests, streaming, key loading and the HTTP proxy.

## Project layout

```
keyswap/
  classify.py   reads a provider's answer: burst 429, quota, bad key, model gone, 503 ...
  pool.py       key registry and the health state machine
  gateway.py    the retry loop behind chat() and stream()
  models.py     model aliases with fallback chains
  sources.py    where keys come from (env vars, .env, files)
  store.py      saves key health across restarts
  proxy.py      the optional HTTP proxy (FastAPI)
  errors.py     the errors above
  __main__.py   python -m keyswap
tests/          the test suite
examples/       a minimal usage example
docs/           v1-to-v2.md: what changed and why
rate_limiter_and_key_swapper_v1/ the original prototype, kept as history
```

## From v1 to v2

The first version (in `rate_limiter_and_key_swapper_v1/`) round-robined keys and benched a key for 12-24 hours on every `429`. Version 2 replaces
that with health-aware selection and handles `429` and `503` separately. The full list of problems and fixes is in
[docs/v1-to-v2.md](docs/v1-to-v2.md).
