import random
import threading

import pytest

from keyswap import KeyPool, KeyState, Outcome, PoolConfig


class FakeClock:
    def __init__(self):
        self.t = 1000.0
        self.wall = 1_700_000_000.0

    def __call__(self):
        return self.t

    def advance(self, s):
        self.t += s
        self.wall += s


def make(n=2, **cfg):
    clock = FakeClock()
    pool = KeyPool(PoolConfig(**cfg), clock=clock, wall_clock=lambda: clock.wall, rng=random.Random(1))
    ids = [pool.add_key(f"secret-{i}", key_id=f"k{i}") for i in range(n)]
    return pool, clock, ids


def state_of(pool, key_id):
    return next(r["state"] for r in pool.snapshot() if r["key_id"] == key_id)


def test_runtime_add_remove_and_validation():
    pool, _, _ = make(1)
    with pytest.raises(ValueError):
        pool.add_key("secret-0")            # duplicate secret
    with pytest.raises(ValueError):
        pool.add_key("bad key with space")
    with pytest.raises(ValueError):
        pool.add_key("another", key_id="k0")  # duplicate id
    assert pool.remove_key("k0") is True
    assert pool.remove_key("k0") is False
    assert pool.acquire() is None


def test_secret_never_printed():
    pool, _, _ = make(1)
    lease = pool.acquire()
    assert "secret-0" not in repr(lease) and "secret-0" not in str(lease.secret)
    assert "secret-0" not in str(pool.snapshot())
    assert "secret-0" not in str(pool.export_state())


def test_picks_least_loaded_then_least_recently_used():
    pool, _, _ = make(3)
    a = pool.acquire()
    b = pool.acquire()
    c = pool.acquire()
    assert {a.key_id, b.key_id, c.key_id} == {"k0", "k1", "k2"}  # spread, none doubled up
    pool.release(b, Outcome.SUCCESS)
    assert pool.acquire().key_id == b.key_id  # the only idle key


def test_429_cools_only_that_key_and_traffic_shifts():
    pool, _, _ = make(2)
    lease = pool.acquire()
    pool.release(lease, Outcome.RATE_LIMITED)
    assert state_of(pool, lease.key_id) == "cooling"
    for _ in range(5):
        nxt = pool.acquire()
        assert nxt.key_id != lease.key_id
        pool.release(nxt, Outcome.SUCCESS)


def test_retry_after_is_honoured_and_capped():
    pool, clock, _ = make(1, max_retry_after_s=100)
    pool.release(pool.acquire(), Outcome.RATE_LIMITED, retry_after=40)
    assert pool.snapshot()[0]["available_in_s"] == 40
    clock.advance(41)
    probe = pool.acquire()
    assert probe.is_probe
    pool.release(probe, Outcome.RATE_LIMITED, retry_after=99999)  # absurd value gets capped
    assert pool.snapshot()[0]["available_in_s"] == 100


def test_backoff_grows_and_resets_after_success():
    pool, clock, _ = make(1, backoff_base_s=10, backoff_max_s=1000)
    waits = []
    for _ in range(3):
        lease = pool.acquire()
        pool.release(lease, Outcome.RATE_LIMITED)
        waits.append(pool.snapshot()[0]["available_in_s"])
        clock.advance(waits[-1] + 1)
    assert waits[0] < waits[1] < waits[2]          # exponential growth
    assert 5 <= waits[0] <= 10                      # jitter: within [base/2, base]
    probe = pool.acquire()
    pool.release(probe, Outcome.SUCCESS)
    assert state_of(pool, "k0") == "active"
    assert pool.snapshot()[0]["consecutive_failures"] == 0


def test_duplicate_429_reports_count_once():
    pool, _, _ = make(1)
    a = pool.acquire()
    b = pool.acquire()  # two requests in flight on the same (only) key
    pool.release(a, Outcome.RATE_LIMITED)
    until_first = pool.snapshot()[0]["available_in_s"]
    pool.release(b, Outcome.RATE_LIMITED)
    snap = pool.snapshot()[0]
    assert snap["consecutive_failures"] == 1
    assert snap["available_in_s"] == until_first


def test_probe_is_a_single_request_not_a_flood():
    pool, clock, _ = make(1)
    pool.release(pool.acquire(), Outcome.RATE_LIMITED)
    clock.advance(10_000)
    probe = pool.acquire()
    assert probe.is_probe and state_of(pool, "k0") == "probing"
    assert pool.acquire() is None                   # nobody else gets through while probing
    pool.release(probe, Outcome.SUCCESS)
    assert state_of(pool, "k0") == "active"
    assert pool.acquire() is not None


def test_failed_probe_goes_back_to_cooling_longer():
    pool, clock, _ = make(1, backoff_base_s=10, backoff_max_s=10_000)
    pool.release(pool.acquire(), Outcome.RATE_LIMITED)
    first = pool.snapshot()[0]["available_in_s"]
    clock.advance(first + 1)
    probe = pool.acquire()
    pool.release(probe, Outcome.RATE_LIMITED)
    assert state_of(pool, "k0") == "cooling"
    assert pool.snapshot()[0]["available_in_s"] > first


def test_neutral_probe_result_does_not_penalise():
    pool, clock, _ = make(1)
    pool.release(pool.acquire(), Outcome.RATE_LIMITED)
    clock.advance(10_000)
    probe = pool.acquire()
    pool.release(probe, Outcome.NEUTRAL)            # e.g. provider returned 503
    assert pool.acquire().is_probe                  # probe-ready again immediately


def test_stuck_probe_is_retried_after_timeout():
    pool, clock, _ = make(1, probe_timeout_s=30)
    pool.release(pool.acquire(), Outcome.RATE_LIMITED)
    clock.advance(10_000)
    pool.acquire()                                  # probe leased, never released (bug / crash)
    assert pool.acquire() is None
    clock.advance(31)
    assert pool.acquire().is_probe


def test_auth_failure_disables_until_reinstated():
    pool, clock, _ = make(2)
    lease = pool.acquire()
    pool.release(lease, Outcome.AUTH_FAILED)
    assert state_of(pool, lease.key_id) == "disabled"
    clock.advance(10**7)                            # time does not heal a bad key
    for _ in range(4):
        other = pool.acquire()
        assert other.key_id != lease.key_id
        pool.release(other, Outcome.SUCCESS)
    assert pool.reinstate(lease.key_id) is True
    assert state_of(pool, lease.key_id) == "active"


def test_quota_exhaustion_gets_long_bench_and_is_not_rerolled():
    pool, clock, _ = make(1, quota_cooldown_min_s=3600, quota_cooldown_max_s=7200)
    a, b = pool.acquire(), pool.acquire()
    pool.release(a, Outcome.QUOTA_EXHAUSTED)
    wait = pool.snapshot()[0]["available_in_s"]
    assert 3600 <= wait <= 7200
    assert state_of(pool, "k0") == "cooling"
    pool.release(b, Outcome.QUOTA_EXHAUSTED)        # duplicate report from a concurrent request
    assert pool.snapshot()[0]["available_in_s"] == wait
    assert pool.acquire() is None


def test_quota_upgrades_a_short_cooldown_to_the_long_bench():
    pool, _, _ = make(1, quota_cooldown_min_s=3600, quota_cooldown_max_s=7200)
    a, b = pool.acquire(), pool.acquire()
    pool.release(a, Outcome.RATE_LIMITED, retry_after=5)
    pool.release(b, Outcome.QUOTA_EXHAUSTED)
    assert pool.snapshot()[0]["available_in_s"] >= 3600


def test_next_available_in():
    pool, clock, _ = make(2)
    assert pool.next_available_in() == 0.0
    a, b = pool.acquire(), pool.acquire()
    pool.release(a, Outcome.RATE_LIMITED, retry_after=50)
    pool.release(b, Outcome.RATE_LIMITED, retry_after=20)
    assert pool.next_available_in() == 20
    empty = KeyPool()
    assert empty.next_available_in() is None


def test_remove_while_in_flight_drains():
    pool, _, _ = make(1)
    lease = pool.acquire()
    assert pool.remove_key("k0") is True
    assert pool.snapshot()[0]["draining"] is True
    assert pool.acquire() is None                   # no new traffic
    pool.release(lease, Outcome.SUCCESS)
    assert pool.snapshot() == []                    # gone once the last request finished


def test_state_survives_restart_without_secrets():
    pool, clock, _ = make(2)
    pool.release(pool.acquire(), Outcome.RATE_LIMITED, retry_after=600)
    bad = pool.acquire()
    pool.release(bad, Outcome.AUTH_FAILED)
    saved = pool.export_state()
    assert "secret" not in str(saved)

    # "restart": brand-new pool, 120s later; the app registers keys again
    clock2 = FakeClock()
    clock2.wall = clock.wall + 120
    fresh = KeyPool(clock=clock2, wall_clock=lambda: clock2.wall)
    fresh.load_state(saved)
    fresh.add_key("secret-0", key_id="k0")
    fresh.add_key("secret-1", key_id="k1")
    states = {r["key_id"]: r for r in fresh.snapshot()}
    cooling = [r for r in states.values() if r["state"] == "cooling"]
    disabled = [r for r in states.values() if r["state"] == "disabled"]
    assert len(cooling) == 1 and len(disabled) == 1
    assert 470 <= cooling[0]["available_in_s"] <= 490   # 600s minus the 120s that passed


def test_concurrent_threads_never_double_book_or_leak_in_flight():
    pool, _, _ = make(4)
    errors = []

    def worker():
        try:
            for _ in range(500):
                lease = pool.acquire()
                if lease is not None:
                    pool.release(lease, Outcome.SUCCESS)
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert all(r["in_flight"] == 0 for r in pool.snapshot())
