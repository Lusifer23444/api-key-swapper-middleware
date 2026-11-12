"""KeyPool: runtime key registry + per-key health state machine.

No HTTP and no web framework in here, so it can be used and tested on its own.

States
------
    ACTIVE    healthy, takes traffic
    COOLING   penalised; waits until its cool-down ends
    PROBING   cool-down ended; exactly ONE real request is testing the key
    DISABLED  invalid/revoked key; stays out until you call reinstate()

Transitions
-----------
    ACTIVE   --429 / quota-->      COOLING
    COOLING  --timer ends-->       (probe-ready) --acquire()--> PROBING
    PROBING  --success-->          ACTIVE            (consecutive failures reset)
    PROBING  --429-->              COOLING           (longer wait: backoff doubles)
    any      --401 / bad key-->    DISABLED

Why this replaces round robin: selection looks at each key's state and load (fewest
in-flight requests, then least recently used), and a key coming back from a cool-down
is tested with one request instead of being flooded.

Thread safety: one threading.Lock guards all state. Critical sections are tiny and never
await, so it is safe from async code too.
"""

from __future__ import annotations

import enum
import hashlib
import itertools
import logging
import random
import threading
import time
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

log = logging.getLogger("keyswap")


class KeyState(str, enum.Enum):
    ACTIVE = "active"
    COOLING = "cooling"
    PROBING = "probing"
    DISABLED = "disabled"


class Outcome(str, enum.Enum):
    """What happened with a leased key. Report exactly one per lease."""
    SUCCESS = "success"
    RATE_LIMITED = "rate_limited"
    QUOTA_EXHAUSTED = "quota_exhausted"
    AUTH_FAILED = "auth_failed"
    NEUTRAL = "neutral"  # says nothing about the key (timeout, 503, bad request, cancelled)


class Secret:
    """Holds an API key but refuses to print it, so it can't leak via logs or tracebacks."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def reveal(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "Secret(****)"

    __str__ = __repr__


@dataclass(frozen=True)
class PoolConfig:
    backoff_base_s: float = 15.0            # first 429 without Retry-After -> about this long
    backoff_max_s: float = 900.0            # exponential backoff never exceeds 15 minutes
    max_retry_after_s: float = 3600.0       # never trust a Retry-After beyond 1 hour
    quota_cooldown_min_s: float = 12 * 3600  # genuine quota exhaustion: long bench, like v1
    quota_cooldown_max_s: float = 24 * 3600
    probe_timeout_s: float = 120.0          # a probe that never reports back is retried after this


class Lease:
    """Permission to use one key for one request. Hand it back via KeyPool.release()."""

    __slots__ = ("key_id", "secret", "is_probe", "_released")

    def __init__(self, key_id: str, secret: Secret, is_probe: bool) -> None:
        self.key_id = key_id
        self.secret = secret
        self.is_probe = is_probe
        self._released = False

    def __repr__(self) -> str:
        return f"Lease(key_id={self.key_id!r}, is_probe={self.is_probe})"


@dataclass
class _Record:
    key_id: str
    secret: Secret
    digest: str                      # sha256 of the secret, only to detect duplicates
    state: KeyState = KeyState.ACTIVE
    until: float = 0.0               # monotonic: cool-down end (COOLING) / probe deadline (PROBING)
    failures: int = 0                # consecutive rate-limit failures, drives backoff
    in_flight: int = 0
    last_used: int = 0               # sequence number, for least-recently-used tie-breaks
    disabled_reason: Optional[str] = None
    removing: bool = False           # draining: finish in-flight work, then delete


def _fingerprint(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()[:8]


class KeyPool:
    def __init__(
        self,
        config: Optional[PoolConfig] = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
        rng: Optional[random.Random] = None,
        on_change: Optional[Callable[[], None]] = None,
    ) -> None:
        self.config = config or PoolConfig()
        self._clock = clock
        self._wall = wall_clock
        self._rng = rng or random.Random()
        self.on_change = on_change
        self._lock = threading.Lock()
        self._records: dict[str, _Record] = {}
        self._pending: dict[str, dict] = {}  # saved state waiting for its key to be registered
        self._seq = itertools.count(1)

    # ------------------------------------------------------------------ #
    # Registry (runtime add / remove, no restart, no .env)
    # ------------------------------------------------------------------ #
    def add_key(self, secret: str, key_id: Optional[str] = None) -> str:
        """Register a key at runtime. Returns its id (use ids, never secrets, everywhere else)."""
        if not isinstance(secret, str):
            raise TypeError("secret must be a string")
        secret = secret.strip()
        if not secret or not secret.isascii() or not secret.isprintable() or any(c.isspace() for c in secret):
            raise ValueError("Key is empty or contains whitespace/control/non-ASCII characters.")
        digest = hashlib.sha256(secret.encode()).hexdigest()
        key_id = key_id or f"key-{_fingerprint(secret)}"
        with self._lock:
            for rec in self._records.values():
                if rec.digest == digest:
                    raise ValueError(f"That key is already registered as {rec.key_id!r}.")
            if key_id in self._records:
                raise ValueError(f"A key with id {key_id!r} already exists.")
            rec = _Record(key_id=key_id, secret=Secret(secret), digest=digest)
            self._records[key_id] = rec
            saved = self._pending.pop(key_id, None)
            if saved:
                self._apply_saved(rec, saved)
        log.info("key %s registered", key_id)
        self._notify()
        return key_id

    def add_keys(self, secrets_: Iterable[str], *, skip_duplicates: bool = False) -> list[str]:
        """Register many keys. With skip_duplicates, keys that are already registered are ignored."""
        ids = []
        for s in secrets_:
            try:
                ids.append(self.add_key(s))
            except ValueError as exc:
                if skip_duplicates and "already registered" in str(exc):
                    continue
                raise
        return ids

    def remove_key(self, key_id: str) -> bool:
        """Remove a key. If requests are still using it, it drains first (no new traffic)."""
        with self._lock:
            rec = self._records.get(key_id)
            if rec is None:
                return False
            if rec.in_flight == 0:
                del self._records[key_id]
            else:
                rec.removing = True
        log.info("key %s removed", key_id)
        self._notify()
        return True

    def reinstate(self, key_id: str) -> bool:
        """Force a key back to ACTIVE (e.g. after you fixed a disabled key). Returns False if unknown."""
        with self._lock:
            rec = self._records.get(key_id)
            if rec is None or rec.removing:
                return False
            rec.state, rec.until, rec.failures, rec.disabled_reason = KeyState.ACTIVE, 0.0, 0, None
        log.warning("key %s manually reinstated", key_id)
        self._notify()
        return True

    # ------------------------------------------------------------------ #
    # Using keys
    # ------------------------------------------------------------------ #
    def acquire(self, exclude: Iterable[str] = ()) -> Optional[Lease]:
        """Pick a key for one request, or None if nothing is usable right now.

        1. A key whose cool-down has ended gets ONE probe request (longest-waiting first).
        2. Otherwise the healthiest ACTIVE key: fewest in-flight requests, then least recently used.
        """
        excluded = set(exclude)
        with self._lock:
            now = self._clock()
            usable = [r for r in self._records.values() if not r.removing and r.key_id not in excluded]

            probe_ready = [r for r in usable
                           if r.state in (KeyState.COOLING, KeyState.PROBING) and r.until <= now]
            if probe_ready:
                rec = min(probe_ready, key=lambda r: r.until)
                rec.state = KeyState.PROBING
                rec.until = now + self.config.probe_timeout_s
                rec.in_flight += 1
                rec.last_used = next(self._seq)
                log.info("key %s cool-down over; sending one probe request", rec.key_id)
                lease = Lease(rec.key_id, rec.secret, is_probe=True)
            else:
                active = [r for r in usable if r.state is KeyState.ACTIVE]
                if not active:
                    return None
                rec = min(active, key=lambda r: (r.in_flight, r.last_used))
                rec.in_flight += 1
                rec.last_used = next(self._seq)
                lease = Lease(rec.key_id, rec.secret, is_probe=False)
        if lease.is_probe:  # only a state change is worth saving; ordinary leases are not
            self._notify()
        return lease

    def confirm_success(self, lease: Lease) -> None:
        """Record that the key worked WITHOUT giving it back yet (used while a stream is still open)."""
        changed = False
        with self._lock:
            rec = self._records.get(lease.key_id)
            if rec is not None and not rec.removing:
                changed = self._apply_outcome(rec, Outcome.SUCCESS, None)
        if changed:
            self._notify()

    def release(self, lease: Lease, outcome: Outcome, retry_after: Optional[float] = None) -> None:
        """Hand a key back and report what happened. Safe to call twice (second call is ignored)."""
        if lease._released:
            return
        lease._released = True
        changed = False
        with self._lock:
            rec = self._records.get(lease.key_id)
            if rec is None:
                return
            rec.in_flight = max(0, rec.in_flight - 1)
            if rec.removing:
                if rec.in_flight == 0:
                    del self._records[rec.key_id]
                return
            changed = self._apply_outcome(rec, outcome, retry_after)
        if changed:
            self._notify()

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #
    def next_available_in(self) -> Optional[float]:
        """0.0 if something can serve right now; else seconds until the soonest key; None if no key can recover."""
        with self._lock:
            now = self._clock()
            waits = []
            for r in self._records.values():
                if r.removing or r.state is KeyState.DISABLED:
                    continue
                if r.state is KeyState.ACTIVE:
                    return 0.0
                waits.append(max(0.0, r.until - now))
            return min(waits) if waits else None

    def snapshot(self) -> list[dict]:
        """Safe-to-log view of every key (ids and states only, never secrets)."""
        with self._lock:
            now = self._clock()
            out = []
            for r in sorted(self._records.values(), key=lambda r: r.key_id):
                waiting = r.state in (KeyState.COOLING, KeyState.PROBING)
                out.append({
                    "key_id": r.key_id,
                    "state": r.state.value,
                    "available_in_s": round(max(0.0, r.until - now), 1) if waiting else 0.0,
                    "in_flight": r.in_flight,
                    "consecutive_failures": r.failures,
                    "disabled_reason": r.disabled_reason,
                    "draining": r.removing,
                })
            return out

    # ------------------------------------------------------------------ #
    # Saving / restoring health (secrets are NEVER included)
    # ------------------------------------------------------------------ #
    def export_state(self) -> dict:
        with self._lock:
            now_m, now_w = self._clock(), self._wall()
            keys = {}
            for r in self._records.values():
                if r.removing or (r.state is KeyState.ACTIVE and r.failures == 0):
                    continue
                entry: dict = {"failures": r.failures}
                if r.state is KeyState.DISABLED:
                    entry.update(state="disabled", reason=r.disabled_reason)
                else:  # COOLING and PROBING both restore as COOLING
                    entry.update(state="cooling", available_at=now_w + max(0.0, r.until - now_m))
                keys[r.key_id] = entry
            return {"version": 1, "keys": keys}

    def load_state(self, state: dict) -> None:
        """Restore saved health. Entries for keys not registered yet wait until add_key() sees them."""
        if not isinstance(state, dict) or state.get("version") != 1:
            return
        with self._lock:
            for key_id, saved in (state.get("keys") or {}).items():
                if not isinstance(saved, dict):
                    continue
                rec = self._records.get(key_id)
                if rec is not None:
                    self._apply_saved(rec, saved)
                else:
                    self._pending[key_id] = saved

    # ------------------------------------------------------------------ #
    # Internals (caller holds the lock)
    # ------------------------------------------------------------------ #
    def _apply_saved(self, rec: _Record, saved: dict) -> None:
        try:
            rec.failures = max(0, int(saved.get("failures", 0)))
            if saved.get("state") == "disabled":
                rec.state, rec.until = KeyState.DISABLED, 0.0
                rec.disabled_reason = saved.get("reason") or "restored as disabled"
            elif saved.get("state") == "cooling":
                remaining = max(0.0, float(saved.get("available_at", 0)) - self._wall())
                rec.state, rec.until = KeyState.COOLING, self._clock() + remaining
        except (TypeError, ValueError):
            log.warning("ignored unreadable saved state for key %s", rec.key_id)

    def _backoff(self, failures: int) -> float:
        cfg = self.config
        raw = min(cfg.backoff_base_s * (2 ** min(max(failures, 1) - 1, 30)), cfg.backoff_max_s)
        return raw / 2 + self._rng.uniform(0, raw / 2)  # "equal jitter": never zero, never in lockstep

    def _apply_outcome(self, rec: _Record, outcome: Outcome, retry_after: Optional[float]) -> bool:
        """Update one key. Returns True if its saved health changed. Idempotent for duplicate reports."""
        now = self._clock()
        cfg = self.config

        if outcome is Outcome.SUCCESS:
            if rec.state is KeyState.PROBING:
                rec.state, rec.until = KeyState.ACTIVE, 0.0
                log.info("key %s probe succeeded; back in rotation", rec.key_id)
                rec.failures = 0
                return True
            if rec.state is KeyState.ACTIVE and rec.failures:
                rec.failures = 0
                return True
            return False

        if rec.state is KeyState.DISABLED:
            return False  # nothing more can happen to a disabled key

        if outcome is Outcome.RATE_LIMITED:
            if rec.state is KeyState.COOLING and rec.until > now:
                return False  # several in-flight requests hit the same 429: count it once
            rec.failures += 1
            if retry_after is not None:
                delay = min(retry_after, cfg.max_retry_after_s)
            else:
                delay = self._backoff(rec.failures)
            rec.state, rec.until = KeyState.COOLING, now + delay
            log.warning("key %s rate limited; cooling %.0fs (failure #%d)", rec.key_id, delay, rec.failures)
            return True

        if outcome is Outcome.QUOTA_EXHAUSTED:
            if rec.state is KeyState.COOLING and rec.until - now >= cfg.quota_cooldown_min_s:
                return False  # already on the long bench
            delay = self._rng.uniform(cfg.quota_cooldown_min_s, cfg.quota_cooldown_max_s)
            rec.state, rec.until = KeyState.COOLING, now + delay
            log.warning("key %s out of quota; benched %.1fh", rec.key_id, delay / 3600)
            return True

        if outcome is Outcome.AUTH_FAILED:
            rec.state, rec.until, rec.disabled_reason = KeyState.DISABLED, 0.0, "auth_failed"
            log.error("key %s rejected as invalid; DISABLED (fix it, then reinstate)", rec.key_id)
            return True

        # NEUTRAL: a probe that hit a provider problem didn't test the key; make it probe-ready again.
        if rec.state is KeyState.PROBING:
            rec.state, rec.until = KeyState.COOLING, now
            return True
        return False

    def _notify(self) -> None:
        cb = self.on_change
        if cb is not None:
            try:
                cb()
            except Exception:  # persistence must never break request handling
                log.exception("on_change callback failed")
