"""End-to-end tests with a fake provider (httpx.MockTransport): no network, no real keys."""

import asyncio
import json

import httpx
import pytest

from fakes import OK, FakeProvider, build, run, states
from keyswap import (
    AllKeysUnavailable, Gateway, JsonStateStore, KeyPool, ModelMap, ModelUnavailable, Provider,
    ProviderUnavailable,
)


def test_happy_path():
    fake = FakeProvider()
    g, _ = build(fake)
    r = run(g.chat({"model": "m", "messages": []}))
    assert r.ok and r.json() == OK and r.attempts == 1


def test_429_swaps_to_next_key_and_cools_the_first():
    fake = FakeProvider({"A": [(429, {"error": "Rate limit: too many requests per minute"}, {"Retry-After": "30"})]})
    g, _ = build(fake)
    # force A to be picked first by making B busy-free but A least recently used: both idle, A first
    r = run(g.chat({"model": "m"}))
    assert r.ok and r.key_id == "B"
    assert states(g) == {"A": "cooling", "B": "active"}
    assert [c[0] for c in fake.calls] == ["A", "B"]


def test_429_uses_retry_after():
    fake = FakeProvider({"A": [(429, {"error": "rate limit"}, {"Retry-After": "42"})]})
    g, _ = build(fake)
    run(g.chat({"model": "m"}))
    a = next(r for r in g.status() if r["key_id"] == "A")
    assert a["available_in_s"] == 42


def test_401_disables_key_permanently():
    fake = FakeProvider({"A": [(401, {"error": "bad key"}, {})]})
    g, _ = build(fake)
    run(g.chat({"model": "m"}))
    assert states(g)["A"] == "disabled"
    for _ in range(5):                       # dead key is never picked again
        assert run(g.chat({"model": "m"})).key_id == "B"
    assert [c[0] for c in fake.calls].count("A") == 1


def test_503_backs_off_provider_and_never_penalises_keys():
    fake = FakeProvider({"A": [(503, {"error": "overloaded"}, {})] * 1, "B": [(503, {"error": "overloaded"}, {})]})
    # both keys 503 first, then recover
    fake.rules["A"] = [(503, {}, {}), (200, OK, {})]
    fake.rules["B"] = [(503, {}, {}), (200, OK, {})]
    g, sleeps = build(fake)
    r = run(g.chat({"model": "m"}))
    assert r.ok
    assert states(g) == {"A": "active", "B": "active"}     # nobody got cooled
    assert len(sleeps) >= 1 and all(s > 0 for s in sleeps)  # provider backoff happened


def test_persistent_503_raises_provider_unavailable_without_cooling_any_key():
    fake = FakeProvider(default=(503, {"error": "down"}, {}))
    g, _ = build(fake)
    with pytest.raises(ProviderUnavailable) as info:
        run(g.chat({"model": "m"}))
    assert info.value.last_status == 503
    assert states(g) == {"A": "active", "B": "active"}
    assert len(fake.calls) == 3  # 1 try + 2 retries, then stop


def test_network_error_is_retried_then_reported():
    def boom(request):
        raise httpx.ConnectError("no route")

    client = httpx.AsyncClient(transport=httpx.MockTransport(boom))
    pool = KeyPool()
    pool.add_key("A", key_id="A")

    async def nosleep(s):
        pass

    g = Gateway(Provider("https://fake.test"), pool, client=client, sleep=nosleep)
    with pytest.raises(ProviderUnavailable) as info:
        run(g.chat({"model": "m"}))
    assert info.value.last_status is None
    assert states(g) == {"A": "active"}


def test_all_keys_cooling_raises_clear_error():
    fake = FakeProvider(default=(429, {"error": "rate limit"}, {"Retry-After": "60"}))
    g, _ = build(fake)
    with pytest.raises(AllKeysUnavailable) as info:
        run(g.chat({"model": "m"}))
    assert info.value.retry_after == pytest.approx(60, abs=1)
    assert len(info.value.keys) == 2


def test_all_keys_disabled_says_nothing_will_recover():
    fake = FakeProvider(default=(401, {"error": "bad"}, {}))
    g, _ = build(fake)
    with pytest.raises(AllKeysUnavailable) as info:
        run(g.chat({"model": "m"}))
    assert info.value.retry_after is None


def test_empty_pool():
    g, _ = build(FakeProvider(), keys=())
    with pytest.raises(AllKeysUnavailable):
        run(g.chat({"model": "m"}))


def test_alias_falls_back_when_model_is_retired():
    """The Gemini 3.6 -> 3.8 story."""
    class Upstream(FakeProvider):
        def __call__(self, request):
            body = json.loads(request.content)
            self.calls.append((request.headers["authorization"][7:], body["model"]))
            if body["model"] == "flash-3.8":
                return httpx.Response(404, json={"error": {"message": "models/flash-3.8 is not found"}})
            return httpx.Response(200, json=OK)

    fake = Upstream()
    models = ModelMap({"fast": ["flash-3.8", "flash-3.6"]})
    g, _ = build(fake, models=models)
    r = run(g.chat({"model": "fast"}))
    assert r.ok and r.model == "flash-3.6"
    assert states(g) == {"A": "active", "B": "active"}      # the keys did nothing wrong
    run(g.chat({"model": "fast"}))
    assert [m for _, m in fake.calls] == ["flash-3.8", "flash-3.6", "flash-3.6"]  # 3.8 not retried


def test_alias_chain_exhausted():
    fake = FakeProvider(default=(404, {"error": {"message": "model not found"}}, {}))
    g, _ = build(fake, models=ModelMap({"fast": ["x", "y"]}))
    with pytest.raises(ModelUnavailable):
        run(g.chat({"model": "fast"}))


def test_raw_model_not_found_is_returned_not_hidden():
    fake = FakeProvider(default=(404, {"error": {"message": "model not found"}}, {}))
    g, _ = build(fake)
    r = run(g.chat({"model": "typo-model"}))
    assert r.status_code == 404


def test_client_errors_pass_through_without_touching_keys():
    fake = FakeProvider(default=(400, {"error": "messages missing"}, {}))
    g, _ = build(fake)
    r = run(g.chat({"model": "m"}))
    assert r.status_code == 400 and states(g) == {"A": "active", "B": "active"}


def test_secret_is_never_in_errors_or_status():
    fake = FakeProvider(default=(429, {"error": "rate limit"}, {}))
    g, _ = build(fake, keys=("sk-super-secret-1", "sk-super-secret-2"), default_ids=True)
    with pytest.raises(AllKeysUnavailable) as info:
        run(g.chat({"model": "m"}))
    assert "super-secret" not in str(info.value) + str(info.value.keys) + str(g.status())


def test_probe_recovers_key_after_cooldown():
    fake = FakeProvider({"A": [(429, {"error": "rate limit"}, {"Retry-After": "5"}), (200, OK, {})]})
    g, _ = build(fake, keys=("A",))
    with pytest.raises(AllKeysUnavailable):
        run(g.chat({"model": "m"}))
    # fast-forward the pool's clock by rewinding the cooldown
    rec = g.pool._records["A"]
    rec.until = g.pool._clock() - 1
    r = run(g.chat({"model": "m"}))
    assert r.ok and states(g) == {"A": "active"}


def test_health_persists_across_restart(tmp_path):
    path = str(tmp_path / "state.json")
    fake = FakeProvider({"A": [(401, {"error": "bad"}, {})]})
    g1, _ = build(fake, store=JsonStateStore(path))
    run(g1.chat({"model": "m"}))
    saved = open(path).read()
    assert "A" in saved and "sk-" not in saved

    g2, _ = build(FakeProvider(), store=JsonStateStore(path))   # new process, same keys re-registered
    assert states(g2)["A"] == "disabled"
    assert run(g2.chat({"model": "m"})).key_id == "B"


def test_concurrent_requests_spread_and_survive_a_bad_key():
    fake = FakeProvider({"A": [(429, {"error": "rate limit"}, {"Retry-After": "30"})]})
    g, _ = build(fake, keys=("A", "B", "C"))

    async def burst():
        return await asyncio.gather(*[g.chat({"model": "m"}) for _ in range(30)])

    results = run(burst())
    assert all(r.ok for r in results)                  # every request was answered, none lost
    assert all(r["in_flight"] == 0 for r in g.status())  # no key left "busy" afterwards
    assert states(g)["A"] == "cooling"
    # Requests that were already in flight on A when its first 429 arrived are unavoidable (how many
    # depends on event-loop timing, so no exact count is asserted), but never more than A's fair share.
    assert [c[0] for c in fake.calls].count("A") <= 10

    # What IS guaranteed: once A is cooling, a second burst never touches it.
    fake.calls.clear()
    results = run(burst())
    assert all(r.ok for r in results)
    assert [c[0] for c in fake.calls].count("A") == 0
