"""Shared test helpers: a scripted fake provider (no network, no real keys)."""

import asyncio
import json
import random

import httpx

from keyswap import Gateway, GatewayConfig, KeyPool, PoolConfig, Provider

OK = {"choices": [{"message": {"content": "hi"}}]}


class FakeProvider:
    """Scripted upstream. `rules` maps an API key to a list of responses; the last one repeats."""

    def __init__(self, rules=None, default=(200, OK, {})):
        self.rules = {k: list(v) for k, v in (rules or {}).items()}
        self.default = default
        self.calls = []      # (key, model)
        self.requests = []   # the raw httpx.Request objects, for header assertions

    def __call__(self, request: httpx.Request) -> httpx.Response:
        auth = request.headers.get("authorization", "") or request.headers.get("x-api-key", "")
        key = auth.removeprefix("Bearer ")
        body = json.loads(request.content)
        self.calls.append((key, body.get("model")))
        self.requests.append(request)
        script = self.rules.get(key)
        if script:
            status, payload, headers = script.pop(0) if len(script) > 1 else script[0]
        else:
            status, payload, headers = self.default
        return httpx.Response(status, json=payload, headers=headers)


def build(fake, keys=("A", "B"), models=None, default_ids=False, provider=None, **gw):
    client = httpx.AsyncClient(transport=httpx.MockTransport(fake))
    pool = KeyPool(PoolConfig(backoff_base_s=10), rng=random.Random(3))
    for k in keys:
        pool.add_key(k, key_id=None if default_ids else k)
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    cfg = gw.pop("config", GatewayConfig(provider_backoff_base_s=1.0))
    g = Gateway(provider or Provider("https://fake.test"), pool, models, client=client, sleep=fake_sleep,
                config=cfg, rng=random.Random(5), **gw)
    return g, sleeps


def run(coro):
    return asyncio.run(coro)


def states(g):
    return {r["key_id"]: r["state"] for r in g.status()}
