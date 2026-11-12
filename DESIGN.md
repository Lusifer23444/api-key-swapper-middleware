# Design notes: keyswap v2

This file explains why keyswap is built the way it is: the problem, the choices, what they cost, and what I would
change next. The user-facing guide is the [README](README.md). The list of v1 problems is in
[docs/v1-to-v2.md](docs/v1-to-v2.md).

## 1. The problem

A real app that calls an AI provider hits three different failures, and each needs a different response:

| Failure | What it means | Wrong response | Right response |
|---|---|---|---|
| `429` burst | One key is busy for about a minute | Bench the key for a day | Cool that key down briefly and try another |
| `429` / `402` quota | The key has used up its allowance | Retry it in a minute | Bench that key for hours |
| `503` / `529` | The provider is struggling | Blame and bench the keys | Back off the provider and keep every key |

v1 treated all three the same way: any `429` benched the key for 12 to 24 hours. A one-minute spike therefore cost a
key for a day. Separating these cases was the main reason for v2.

## 2. Key decisions

### 2.1 Health-aware selection instead of round robin
Round robin does not know whether a key is cooling down, probing, or disabled. v2 gives each key a state
(`ACTIVE`, `COOLING`, `PROBING`, `DISABLED`). A request goes to the active key with the fewest requests in flight,
ties going to the least recently used key.

**Cost:** more code, and more states to test. The state machine is in `keyswap/pool.py`, and it has its own tests.

### 2.2 One probe before full traffic
When a cool-down ends, the key is not flooded. Exactly one request tests it. If that request succeeds, the key returns
to full use. If it gets a `429`, the cool-down gets longer. A probe that never reports back is retried after
`probe_timeout_s` (120 s), so a lost request cannot leave a key stuck.

**Why:** a key that just recovered from a rate limit is most likely to hit it again the moment it gets a full load.

**Cost:** a recovering key serves one request later than it could. I accepted that delay.

### 2.3 Backoff for bursts, a long bench only for quota
- A `429` with `Retry-After` is honoured, capped at 1 hour.
- A `429` without it: about 15 seconds, doubling on each consecutive failure up to 15 minutes, with jitter.
- Quota exhaustion: a random 12 to 24 hours.

Several requests that report the same `429` at once count as one failure. Without that, a burst of ten concurrent
requests would bench the key ten times over.

**Why jitter:** keys that were benched together would otherwise come back together.

### 2.4 Provider outages do not touch key health
`502`, `503`, `504` and `529` back off the provider as a whole and retry up to two times. No key is penalised.

**Why:** an outage is not the key's fault. Benching all keys during an outage would turn a brief provider problem into
a full app outage, and it is the failure v1 had.

### 2.5 Classification is heuristic
`keyswap/classify.py` decides what a response means. The status code gives most of the answer. For `402` and `403`, and
for some `429`s, it reads the body for words such as "quota" or "billing".

**Trade-off:** a provider that changes its wording can be misread. A quota error might look like a burst and get a
short cool-down, and the key would then be retried too early. The fallback is the short cool-down, so the failure is
mild and repeated `429`s still escalate. The limitation is listed in the README.

### 2.6 Streams swap keys before the first byte
Once a streamed answer has begun, the client has received part of it. The gateway can switch keys only before that
point. After it, the stream can only be cut short.

**Trade-off:** a quota error that arrives in the middle of a stream cannot be recovered. The alternative, replaying the
request on another key, would send duplicated or corrupted text to the caller.

### 2.7 Secrets are never printed
A `Secret` wrapper refuses to show its value in `repr` or `str`. Key ids are fingerprints (a hash prefix). Logs, status
output and errors show ids only. The health file stores ids and times, never secrets.

**Trade-off:** an app that wants to save key health must register its keys again on each start. That is intentional.

### 2.8 Keys can be added and removed while running
A pool that is fixed at startup needs a restart for every key change. v2 allows `add_key`, `remove_key` and the
`/proxy/keys` endpoints. Removal lets in-flight requests finish first.

**Trade-off:** keys added over HTTP live in memory only, so they are gone after a restart unless the app loads them
again from its own source.

### 2.9 One provider per gateway
Keys for different providers have different error formats and different rules. Mixing them in one pool would make the
classification rules ambiguous.

**Trade-off:** failing over from one provider to another is not supported. An app that wants that must run two
gateways and choose between them itself.

### 2.10 Library first, proxy second
The core logic is a Python library with no web framework. The proxy (FastAPI) is an optional extra. This means the
logic can be tested directly, and an app in Python does not need to run a second process.

## 3. What is deliberately not built

- **Multi-process state.** Each worker would keep its own view of key health. A shared store such as Redis would fix
  this, but it adds a dependency and a failure mode. For a single-process deployment it is not needed.
- **Token-per-minute budgeting.** The gateway reacts to the provider's `429`s. It does not predict them. Predicting
  would need token counts for every request, which is more work than the problem needs right now.
- **Automatic model discovery.** Aliases handle renames. A provider changing its request format still needs a code change.

## 4. How it is tested

The suite has 77 tests, which use a fake provider. They cover classification, the pool state machine, the gateway's
retry loop, streaming, key loading, and the HTTP proxy.

The tests check behaviour, not the internal details of the code. Where the state machine is involved, the tests check
the transitions in the table at the top of `pool.py`.

Not yet covered: a live run against a real provider. The presets for Anthropic and Gemini have not been checked against
their live APIs.
