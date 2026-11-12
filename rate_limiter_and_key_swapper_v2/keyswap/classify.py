"""Turn an upstream HTTP response into a decision.

Pure functions only (no I/O, no state), so every rule here is trivially unit-testable.

The key idea of v2: 429 and 503 are different problems.
    429 -> THIS KEY is over its limit      -> cool down that key
    503 -> THE PROVIDER is overloaded      -> back off the provider, never punish keys
"""

from __future__ import annotations

import enum
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Mapping, Optional

_BODY_LIMIT = 64 * 1024  # only the start of an error body is ever inspected


class Kind(str, enum.Enum):
    OK = "ok"
    RATE_LIMITED = "rate_limited"        # short burst limit on one key (429)
    QUOTA_EXHAUSTED = "quota_exhausted"  # long-term quota / billing problem on one key
    AUTH_FAILED = "auth_failed"          # key is invalid or revoked -> disable it
    MODEL_NOT_FOUND = "model_not_found"  # model was renamed / retired
    PROVIDER_ERROR = "provider_error"    # 502/503/504/529: provider-wide trouble
    PASSTHROUGH = "passthrough"          # not our business (400, 500, ...): hand it to the caller


@dataclass(frozen=True)
class Verdict:
    kind: Kind
    retry_after: Optional[float] = None  # seconds, from the provider's Retry-After header


# Heuristics. Providers word these differently, so they are deliberately plain and easy to extend.
_PER_MINUTE = (b"perminute", b"per minute", b"per_minute", b"rate limit", b"rate_limit", b"too many requests")
_QUOTA = (b"insufficient_quota", b"billing", b"credit balance", b"perday", b"per day", b"daily",
          b"exceeded your current quota")
_BAD_KEY = (b"api key not valid", b"invalid api key", b"incorrect api key", b"api_key_invalid",
            b"invalid x-api-key", b"authentication_error")
_MODEL_GONE = (b"not found", b"does not exist", b"deprecated", b"decommissioned",
               b"no longer available", b"not supported", b"unknown model", b"invalid model")

PROVIDER_ERROR_STATUSES = frozenset({502, 503, 504, 529})


def parse_retry_after(headers: Mapping[str, str], *, wall_now: Optional[float] = None) -> Optional[float]:
    """Seconds to wait according to Retry-After (seconds or HTTP-date) or retry-after-ms, else None."""
    lowered = {k.lower(): v for k, v in headers.items()}
    ms = lowered.get("retry-after-ms")
    if ms:
        try:
            value = float(ms) / 1000.0
            return value if value >= 0 else None
        except ValueError:
            pass
    raw = lowered.get("retry-after")
    if not raw:
        return None
    raw = raw.strip()
    try:
        value = float(raw)
        return value if value >= 0 else None
    except ValueError:
        pass
    try:  # HTTP-date form
        when = parsedate_to_datetime(raw)
        now = wall_now if wall_now is not None else time.time()
        return max(0.0, when.timestamp() - now)
    except (TypeError, ValueError):
        return None


def classify(status: int, body: bytes, headers: Mapping[str, str]) -> Verdict:
    low = body[:_BODY_LIMIT].lower()

    if status < 400:
        return Verdict(Kind.OK)

    if status == 429:
        retry_after = parse_retry_after(headers)
        looks_per_minute = any(m in low for m in _PER_MINUTE)
        looks_quota = any(m in low for m in _QUOTA)
        if looks_quota and not looks_per_minute:
            return Verdict(Kind.QUOTA_EXHAUSTED, retry_after)
        return Verdict(Kind.RATE_LIMITED, retry_after)

    if status == 402:
        return Verdict(Kind.QUOTA_EXHAUSTED)

    if status == 401:
        return Verdict(Kind.AUTH_FAILED)

    if status in (400, 403):
        if any(m in low for m in _BAD_KEY):
            return Verdict(Kind.AUTH_FAILED)
        if status == 403 and any(m in low for m in _QUOTA):
            return Verdict(Kind.QUOTA_EXHAUSTED)
        if status == 400 and b"model" in low and any(m in low for m in _MODEL_GONE):
            return Verdict(Kind.MODEL_NOT_FOUND)
        return Verdict(Kind.PASSTHROUGH)

    if status == 404 and b"model" in low:
        return Verdict(Kind.MODEL_NOT_FOUND)

    if status in PROVIDER_ERROR_STATUSES:
        return Verdict(Kind.PROVIDER_ERROR, parse_retry_after(headers))

    return Verdict(Kind.PASSTHROUGH)
