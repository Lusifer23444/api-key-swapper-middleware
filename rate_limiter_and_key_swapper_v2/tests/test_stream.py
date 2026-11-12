"""Streaming: keys are swapped BEFORE the first byte; mid-stream failures never blame the key."""

import json
import random

import httpx
import pytest

from fakes import OK, build, run, states
from keyswap import AllKeysUnavailable, Gateway, KeyPool, Provider

SSE = [b'data: {"n":1}\n\n', b'data: {"n":2}\n\n', b"data: [DONE]\n\n"]


class StreamUpstream:
    """Streams SSE chunks for 200, otherwise returns the scripted error. `rules` maps key -> status."""

    def __init__(self, rules=None, break_after=None):
        self.rules = rules or {}
        self.break_after = break_after
        self.keys = []

    def __call__(self, request):
        key = request.headers["authorization"][7:]
        self.keys.append(key)
        status = self.rules.get(key, 200)
        if status != 200:
            return httpx.Response(status, json={"error": "rate limit" if status == 429 else "bad"},
                                  headers={"Retry-After": "30"} if status == 429 else {})

        async def chunks():
            for i, c in enumerate(SSE):
                if self.break_after is not None and i == self.break_after:
                    raise httpx.ReadError("connection dropped")
                yield c

        return httpx.Response(200, content=chunks(), headers={"content-type": "text/event-stream"})


def make(upstream, keys=("A", "B")):
    client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    pool = KeyPool(rng=random.Random(1))
    for k in keys:
        pool.add_key(k, key_id=k)

    async def nosleep(s):
        pass

    return Gateway(Provider("https://fake.test"), pool, client=client, sleep=nosleep)


async def collect(gw, payload):
    async with gw.stream(payload) as s:
        data = b"".join([c async for c in s.aiter_bytes()])
        return s, data


def test_stream_relays_chunks_in_order():
    gw = make(StreamUpstream())
    s, data = run(collect(gw, {"model": "m", "stream": True}))
    assert s.ok and data == b"".join(SSE)
    assert all(r["in_flight"] == 0 for r in gw.status())   # lease returned when the stream ends


def test_429_is_swapped_before_any_byte_is_sent():
    up = StreamUpstream({"A": 429})
    gw = make(up)
    s, data = run(collect(gw, {"model": "m", "stream": True}))
    assert s.ok and s.key_id == "B" and data == b"".join(SSE)
    assert states(gw) == {"A": "cooling", "B": "active"}
    assert up.keys == ["A", "B"]


def test_401_disables_key_on_stream_path_too():
    gw = make(StreamUpstream({"A": 401}))
    run(collect(gw, {"model": "m", "stream": True}))
    assert states(gw)["A"] == "disabled"


def test_non_2xx_comes_back_as_a_small_buffered_body():
    gw = make(StreamUpstream({"A": 400, "B": 400}))
    s, data = run(collect(gw, {"model": "m", "stream": True}))
    assert s.status_code == 400 and json.loads(data) == {"error": "bad"}
    assert states(gw) == {"A": "active", "B": "active"}


def test_all_keys_cooling_raises_before_streaming():
    gw = make(StreamUpstream({"A": 429, "B": 429}))
    with pytest.raises(AllKeysUnavailable):
        run(collect(gw, {"model": "m", "stream": True}))


def test_midstream_failure_truncates_but_does_not_penalise_key():
    gw = make(StreamUpstream(break_after=1), keys=("A",))
    s, data = run(collect(gw, {"model": "m", "stream": True}))
    assert s.failed and data == SSE[0]                     # got what arrived, then stopped cleanly
    assert states(gw) == {"A": "active"}                   # a dropped connection says nothing about the key
    assert gw.status()[0]["in_flight"] == 0


def test_leaving_the_stream_early_still_releases_the_key():
    gw = make(StreamUpstream(), keys=("A",))

    async def leave_early():
        async with gw.stream({"model": "m", "stream": True}) as s:
            async for _ in s.aiter_bytes():
                break                                        # client hung up after the first chunk

    run(leave_early())
    assert gw.status()[0]["in_flight"] == 0


def test_probe_succeeds_via_stream_and_key_recovers():
    up = StreamUpstream()
    gw = make(up, keys=("A",))
    gw.pool.release(gw.pool.acquire(), __import__("keyswap").Outcome.RATE_LIMITED, retry_after=5)
    gw.pool._records["A"].until = gw.pool._clock() - 1     # cool-down over: next request is the probe
    run(collect(gw, {"model": "m", "stream": True}))
    assert states(gw) == {"A": "active"}


def test_stream_time_cap_truncates(monkeypatch):
    from keyswap import GatewayConfig
    gw = make(StreamUpstream())
    gw.config = GatewayConfig(max_stream_s=-1)               # already expired after the first chunk
    s, data = run(collect(gw, {"model": "m", "stream": True}))
    assert s.truncated and data == SSE[0]
