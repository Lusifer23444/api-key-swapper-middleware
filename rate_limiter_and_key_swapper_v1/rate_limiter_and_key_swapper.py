"""
rate_limiter_and_key_swapper.py
================================

Standalone API rate-limiter / auto key-swapper reverse proxy (hardened build).

Flow
----
    client --POST /proxy/v1/chat--> [this proxy] --POST {UPSTREAM}--> AI provider
                                        |
                                        |  upstream says 429 / quota exhausted?
                                        |    -> bench that key for 12-24h (cool-down)
                                        |    -> retry immediately with next healthy key
                                        |  every key benched?
                                        |    -> 503 + Retry-After
                                        v
                                    client gets the first non-rate-limited response

Run (Python 3.9+)
-----------------
    pip install fastapi httpx uvicorn            # python-dotenv optional (.env support)
    export API_KEYS="key1,key2,key3"
    export PROXY_AUTH_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
    export UPSTREAM_BASE_URL="https://api.openai.com"
    uvicorn rate_limiter_and_key_swapper:app --host 127.0.0.1 --port 8000

Environment variables
---------------------
  Required
    API_KEYS                  comma-separated upstream keys
    PROXY_AUTH_TOKEN          clients must send "Authorization: Bearer <token>" (scheme is
                              case-insensitive per RFC 7235; the token itself is exact-match).
                              Also gates the admin endpoints. The proxy REFUSES TO START
                              without it unless ALLOW_UNAUTHENTICATED=1 (local dev only).
  Upstream
    UPSTREAM_BASE_URL         default https://api.openai.com (plain http:// is only accepted
                              for localhost, or with ALLOW_INSECURE_UPSTREAM=1)
    UPSTREAM_CHAT_PATH        default /v1/chat/completions
    UPSTREAM_AUTH_HEADER      default Authorization   (use x-api-key for Anthropic-style APIs)
    UPSTREAM_AUTH_SCHEME      default Bearer          (set empty to send the raw key)
    FORWARD_HEADERS           default anthropic-version,anthropic-beta,openai-beta
                              client headers copied upstream (everything else is dropped)
  Cool-down
    COOLDOWN_MIN_HOURS        default 12
    COOLDOWN_MAX_HOURS        default 24
  Limits / timeouts
    UPSTREAM_TIMEOUT_SECONDS  default 60    max silence on connect/read/write (per attempt)
    REQUEST_DEADLINE_SECONDS  default 120   hard ceiling for headers (all retries combined)
                                            + non-streaming body
    MAX_STREAM_SECONDS        default 900   hard ceiling for one streamed response
    MAX_CONNECTIONS           default 200   upstream connection-pool size (raise for heavy concurrency)
    POOL_TIMEOUT_SECONDS      default 5     max wait for a free pooled connection before answering 503.
                                            Keep it BELOW your clients' own timeout so they receive this
                                            clean 503 + Retry-After rather than timing out first.
    MAX_REQUEST_BODY_BYTES    default 10485760 (10 MiB)
    MAX_RESPONSE_BYTES        default 33554432 (32 MiB), non-streaming responses only
  Process
    DISABLE_DOTENV            default off; set 1 to skip the automatic .env load, so ONLY real
                              environment variables configure the proxy (hermetic tests,
                              containers). A .env can add settings but can never express "unset".
    HOST                      default 127.0.0.1 (only used by `python this_file.py`)
    PORT                      default 8000

Endpoints
---------
    POST /proxy/v1/chat       the proxied call
    GET  /proxy/health        counts only; fingerprints/countdowns added when admin token sent
  Admin (all require "Authorization: Bearer $PROXY_AUTH_TOKEN")
    GET  /proxy/keys/benched  list keys currently in cool-down (fingerprints + countdown)
    POST /proxy/keys/reinject force a benched key back into rotation immediately
        {"key_id": "<8-char fingerprint from the listing>"}   preferred
        {"key": "<exact raw key>"}                            avoid: secret travels in the body
        {"all": true}                                         reinject every benched key

Design notes
------------
* State is in-memory and per-process. Run ONE worker (or move KeyPool to Redis) --
  with N uvicorn workers each would keep its own cool-down table. A restart clears
  all cool-downs (a benched key is simply re-benched on its next 429).
* The cool-down table is keyed by key, so it can never hold more than len(keys)
  entries: no unbounded growth. Deadlines use time.monotonic(), so wall-clock jumps
  (NTP, DST, VM resume) cannot shorten or lengthen a cool-down.
* Cool-down durations are jittered inside [min, max] hours so that keys benched
  together don't all return to service at the same instant (thundering herd).
* Upstream credentials are never taken from the client. Request headers are
  WHITELISTED (never copied wholesale), so the proxy token, cookies, X-Forwarded-*
  etc. cannot leak upstream. Response headers that identify the upstream account
  (Set-Cookie, *-organization, ...) are stripped before reaching the client.
* The upstream URL is fixed by configuration; clients only control the JSON body, so
  there is no SSRF or path-traversal surface. Redirects are never followed (a redirect
  would otherwise carry the pooled key to a third-party host).
* Timeouts never rotate keys: a stall says nothing about a key's quota, so it
  returns 504 and leaves the key in rotation. Only 429 (or 402/403 with a quota
  body) benches a key.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import re
import secrets
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Optional
from urllib.parse import urlsplit

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.requests import ClientDisconnect

try:  # optional .env support; harmless if python-dotenv isn't installed
    if os.getenv("DISABLE_DOTENV", "").strip().lower() not in {"1", "true", "yes", "on"}:
        from dotenv import load_dotenv

        load_dotenv()  # never overrides variables that are already set in the real environment
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("key_swapper")


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
_HEADER_TOKEN_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")  # RFC 9110 "token"

# Headers a client may never make us forward, whatever FORWARD_HEADERS says.
_NEVER_FORWARD = frozenset({
    "authorization", "proxy-authorization", "cookie", "host", "content-length",
    "x-api-key", "api-key", "x-goog-api-key",
})

_ERROR_BODY_LIMIT = 64 * 1024  # 402/403 bodies are read only to classify them


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        value = float(raw)
    except ValueError:
        raise RuntimeError(f"{name} must be a number, got {raw!r}") from None
    if not math.isfinite(value):
        raise RuntimeError(f"{name} must be finite, got {raw!r}")
    return value


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return int(raw)
    except ValueError:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from None


def _env_bool(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    api_keys: tuple[str, ...]
    upstream_base_url: str
    upstream_chat_path: str
    auth_header: str
    auth_scheme: str
    forward_headers: tuple[str, ...]
    cooldown_min_s: float
    cooldown_max_s: float
    timeout_s: float
    request_deadline_s: float
    max_stream_s: float
    max_body_bytes: int
    max_response_bytes: int
    proxy_auth_token: Optional[str]
    max_connections: int = 200
    pool_timeout_s: float = 5.0

    @classmethod
    def from_env(cls) -> "Settings":
        # Split, trim, drop blanks, de-duplicate while preserving order.
        # A duplicate key would otherwise get two round-robin slots and
        # cool-down tracking would double count it.
        raw = os.getenv("API_KEYS", "")
        keys = tuple(dict.fromkeys(k.strip() for k in raw.split(",") if k.strip()))
        if not keys:
            raise RuntimeError("API_KEYS is empty. Set it to a comma-separated list, e.g. API_KEYS=k1,k2,k3")
        for k in keys:
            # Catches stray quotes/newlines/inner spaces from a bad .env paste, and
            # guarantees a key can never smuggle extra header bytes upstream.
            if not (k.isascii() and k.isprintable()) or any(c.isspace() for c in k):
                raise RuntimeError("API_KEYS contains an entry with whitespace, control or non-ASCII characters.")

        lo = _env_float("COOLDOWN_MIN_HOURS", 12) * 3600
        hi = _env_float("COOLDOWN_MAX_HOURS", 24) * 3600
        if lo <= 0 or hi < lo:
            raise RuntimeError("Invalid cool-down window: need 0 < COOLDOWN_MIN_HOURS <= COOLDOWN_MAX_HOURS")

        base = os.getenv("UPSTREAM_BASE_URL", "https://api.openai.com").strip().rstrip("/")
        parts = urlsplit(base)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise RuntimeError("UPSTREAM_BASE_URL must be an absolute http(s) URL.")
        is_local = parts.hostname in {"localhost", "127.0.0.1", "::1"}
        if parts.scheme == "http" and not is_local and not _env_bool("ALLOW_INSECURE_UPSTREAM"):
            raise RuntimeError(
                "UPSTREAM_BASE_URL uses plain http:// for a non-local host, which would send your API keys "
                "in cleartext. Use https://, or set ALLOW_INSECURE_UPSTREAM=1 if you really mean it."
            )

        auth_header = os.getenv("UPSTREAM_AUTH_HEADER", "Authorization").strip()
        if not _HEADER_TOKEN_RE.match(auth_header):
            raise RuntimeError("UPSTREAM_AUTH_HEADER is not a valid HTTP header name.")

        forward = tuple(
            h.strip().lower()
            for h in os.getenv("FORWARD_HEADERS", "anthropic-version,anthropic-beta,openai-beta").split(",")
            if h.strip()
        )
        for h in forward:
            if not _HEADER_TOKEN_RE.match(h):
                raise RuntimeError(f"FORWARD_HEADERS contains an invalid header name: {h!r}")
        forbidden = set(forward) & (_NEVER_FORWARD | {auth_header.lower()})
        if forbidden:
            raise RuntimeError(f"FORWARD_HEADERS may not include credential/hop headers: {sorted(forbidden)}")

        timeout_s = _env_float("UPSTREAM_TIMEOUT_SECONDS", 60)
        deadline_s = _env_float("REQUEST_DEADLINE_SECONDS", 120)
        max_stream_s = _env_float("MAX_STREAM_SECONDS", 900)
        if min(timeout_s, deadline_s, max_stream_s) <= 0:
            raise RuntimeError("Timeout settings must be > 0.")
        max_body = _env_int("MAX_REQUEST_BODY_BYTES", 10 * 1024 * 1024)
        max_resp = _env_int("MAX_RESPONSE_BYTES", 32 * 1024 * 1024)
        if max_body <= 0 or max_resp <= 0:
            raise RuntimeError("Size limits must be > 0.")
        max_conns = _env_int("MAX_CONNECTIONS", 200)
        pool_timeout_s = _env_float("POOL_TIMEOUT_SECONDS", 5)
        if max_conns <= 0 or pool_timeout_s <= 0:
            raise RuntimeError("MAX_CONNECTIONS and POOL_TIMEOUT_SECONDS must be > 0.")

        token = os.getenv("PROXY_AUTH_TOKEN") or None
        if token is None and not _env_bool("ALLOW_UNAUTHENTICATED"):
            raise RuntimeError(
                "PROXY_AUTH_TOKEN is not set. Without it anyone who can reach this port can spend your API keys. "
                "Set it, or set ALLOW_UNAUTHENTICATED=1 for local development only."
            )
        if token is None:
            log.warning("ALLOW_UNAUTHENTICATED is on: /proxy/v1/chat is OPEN and admin endpoints are disabled")
        elif len(token) < 16:
            log.warning("PROXY_AUTH_TOKEN is shorter than 16 characters; use a long random value")

        return cls(
            api_keys=keys,
            upstream_base_url=base,
            upstream_chat_path="/" + os.getenv("UPSTREAM_CHAT_PATH", "/v1/chat/completions").lstrip("/"),
            auth_header=auth_header,
            auth_scheme=os.getenv("UPSTREAM_AUTH_SCHEME", "Bearer"),
            forward_headers=forward,
            cooldown_min_s=lo,
            cooldown_max_s=hi,
            timeout_s=timeout_s,
            request_deadline_s=deadline_s,
            max_stream_s=max_stream_s,
            max_body_bytes=max_body,
            max_response_bytes=max_resp,
            proxy_auth_token=token,
            max_connections=max_conns,
            pool_timeout_s=pool_timeout_s,
        )


# --------------------------------------------------------------------------- #
# Key pool: round-robin + cool-down tracker
# --------------------------------------------------------------------------- #
def fingerprint(key: str) -> str:
    """Stable, non-reversible short ID so logs/status never expose secrets."""
    return hashlib.sha256(key.encode()).hexdigest()[:8]


class KeyPool:
    """
    Async-safe round-robin pool with a cool-down ("penalty box") tracker.

    Invariants (all mutations happen under self._lock, and no `await` is ever
    performed while holding it other than the lock itself):
      * `_cooldowns[key]` holds the MONOTONIC clock time at which the key
        becomes eligible again. A key is "active" iff it is absent from the dict.
        The dict is keyed by key, so it is bounded by len(keys): it cannot grow.
      * `_cursor` is the index of the next key to try; it advances past every key
        handed out, which yields fair rotation across concurrent requests.
    """

    def __init__(self, keys: tuple[str, ...], cooldown_min_s: float, cooldown_max_s: float) -> None:
        self._keys = list(keys)
        self._cooldowns: dict[str, float] = {}
        self._cursor = 0
        self._lock = asyncio.Lock()
        self._cd_min = cooldown_min_s
        self._cd_max = cooldown_max_s

    # -- internal (caller MUST hold the lock) ------------------------------- #
    def _release_expired(self, now: float) -> None:
        """Lazily return keys whose cool-down window has elapsed to the active loop."""
        for key in [k for k, until in self._cooldowns.items() if until <= now]:
            del self._cooldowns[key]
            log.info("key %s cool-down elapsed; back in rotation", fingerprint(key))

    # -- public API --------------------------------------------------------- #
    async def acquire(self, exclude: frozenset[str] = frozenset()) -> Optional[str]:
        """
        Return the next healthy key in round-robin order, or None if every key is
        cooling down (or excluded). Scans at most len(keys) slots, so it is O(n)
        and can never spin.
        """
        async with self._lock:
            now = time.monotonic()
            self._release_expired(now)
            n = len(self._keys)
            for offset in range(n):
                idx = (self._cursor + offset) % n
                key = self._keys[idx]
                if key in self._cooldowns or key in exclude:
                    continue
                self._cursor = (idx + 1) % n  # next caller starts after this key
                return key
            return None

    async def bench(self, key: str) -> None:
        """
        Pull `key` out of rotation for a jittered 12-24h window.

        Idempotent: if several in-flight requests all hit 429 on the same key,
        only the first call sets the timer; later ones must not extend it.
        """
        async with self._lock:
            if key in self._cooldowns:
                return
            duration = random.uniform(self._cd_min, self._cd_max)
            self._cooldowns[key] = time.monotonic() + duration
            log.warning(
                "key %s rate-limited/exhausted -> cool-down %.1fh (%d/%d keys now benched)",
                fingerprint(key), duration / 3600, len(self._cooldowns), len(self._keys),
            )

    async def seconds_until_available(self) -> Optional[int]:
        """Seconds until the soonest benched key returns; None if a key is already healthy."""
        async with self._lock:
            now = time.monotonic()
            self._release_expired(now)
            if len(self._cooldowns) < len(self._keys):
                return None
            return max(1, int(min(self._cooldowns.values()) - now))

    async def snapshot(self) -> dict:
        """Operational view for the health endpoint (fingerprints only, no secrets)."""
        async with self._lock:
            now = time.monotonic()
            self._release_expired(now)
            return {
                "total_keys": len(self._keys),
                "active_keys": len(self._keys) - len(self._cooldowns),
                "cooling_down": [
                    {"key": fingerprint(k), "available_in_seconds": max(0, int(until - now))}
                    for k, until in sorted(self._cooldowns.items(), key=lambda kv: kv[1])
                ],
            }

    async def reinject(self, key_id: Optional[str] = None, raw_key: Optional[str] = None) -> tuple[str, Optional[str]]:
        """
        Manually pull ONE benched key out of cool-down, bypassing its timer.

        The key is identified either by its public fingerprint (`key_id`) or by
        exact match on the raw key (`raw_key`). Returns (outcome, fingerprint):
            "reinjected"  - key was benched and is now active again
            "not_benched" - key exists but is already healthy (nothing changed)
            "unknown_key" - no pool key matches the identifier
            "ambiguous"   - fingerprint matched >1 key (hash-prefix collision);
                            caller should supply the exact key instead

        No cursor adjustment is needed: rotation scans every slot and skips only
        keys present in `_cooldowns`, so deleting the entry is what re-admits it.
        """
        async with self._lock:
            self._release_expired(time.monotonic())  # don't report naturally-expired keys as benched

            if raw_key is not None:
                # compare_digest: exact match without leaking prefix length via timing.
                matches = [k for k in self._keys if secrets.compare_digest(k.encode(), raw_key.encode())]
            else:
                wanted = (key_id or "").strip().lower()
                matches = [k for k in self._keys if fingerprint(k) == wanted]

            if not matches:
                return "unknown_key", None
            if len(matches) > 1:
                return "ambiguous", None

            key = matches[0]
            fp = fingerprint(key)
            if key not in self._cooldowns:
                return "not_benched", fp

            del self._cooldowns[key]
            log.warning("key %s MANUALLY reinjected into rotation (cool-down bypassed)", fp)
            return "reinjected", fp

    async def reinject_all(self) -> list[str]:
        """Clear the whole penalty box in one atomic step; returns the fingerprints released."""
        async with self._lock:
            self._release_expired(time.monotonic())
            released = [fingerprint(k) for k in self._cooldowns]
            self._cooldowns.clear()
            if released:
                log.warning("ALL benched keys MANUALLY reinjected: %s", ", ".join(released))
            return released


# --------------------------------------------------------------------------- #
# Upstream error classification
# --------------------------------------------------------------------------- #
# Providers signal exhausted quota in different ways. 429 is canonical; some
# return 402/403 with a quota/billing body instead.
#
# NOTE: every 429 benches the key for 12-24h, including short per-minute bursts.
# That is the documented policy here; if your provider's 429s are usually
# transient, honour Retry-After instead of the fixed window.
_QUOTA_MARKERS = (
    b"insufficient_quota", b"quota", b"billing", b"credit balance",
    b"resource_exhausted", b"rate limit", b"rate_limit",
)


def is_key_exhausted(status: int, body: bytes) -> bool:
    if status == 429:
        return True
    if status in (402, 403):
        lowered = body[:_ERROR_BODY_LIMIT].lower()
        return any(m in lowered for m in _QUOTA_MARKERS)
    return False


# --------------------------------------------------------------------------- #
# Proxy helpers
# --------------------------------------------------------------------------- #
# Hop-by-hop / re-computed headers, plus headers that would leak upstream account
# identity or set cookies on the proxy's own origin.
_STRIP_RESPONSE_HEADERS = {
    "connection", "keep-alive", "transfer-encoding", "content-length",
    "content-encoding", "te", "trailer", "upgrade", "proxy-authenticate",
    "set-cookie", "set-cookie2",
    "openai-organization", "openai-project", "anthropic-organization-id",
}


def clean_response_headers(headers: httpx.Headers) -> dict[str, str]:
    return {k: v for k, v in headers.items() if k.lower() not in _STRIP_RESPONSE_HEADERS}


def error_response(status: int, err_type: str, message: str, headers: Optional[dict] = None, **extra) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"error": {"type": err_type, "message": message, **extra}},
        headers=headers,
    )


def upstream_failure(exc: BaseException) -> JSONResponse:
    """Map a transport-level failure to a client response. Never benches a key."""
    if isinstance(exc, (asyncio.TimeoutError, httpx.TimeoutException)):
        log.error("upstream timeout: %s", type(exc).__name__)
        return error_response(504, "upstream_timeout", "Upstream provider timed out.")
    log.error("upstream transport error: %s", exc)
    return error_response(502, "upstream_unreachable", "Could not reach upstream provider.")


def build_upstream_headers(settings: Settings, key: str, request: Request) -> dict[str, str]:
    """
    Whitelist approach: forward only content negotiation headers plus the explicit
    FORWARD_HEADERS list, and inject the pooled credential. Everything else the
    client sent -- Authorization (the proxy token), Cookie, X-Forwarded-For,
    Proxy-Authorization, arbitrary X-* -- is dropped.
    `Accept-Encoding: identity` keeps upstream bytes decoded-as-is so streaming
    passthrough and error-body inspection need no decompression handling.
    """
    headers = {
        "Content-Type": request.headers.get("content-type", "application/json"),
        "Accept": request.headers.get("accept", "application/json"),
        "Accept-Encoding": "identity",
    }
    for name in settings.forward_headers:
        value = request.headers.get(name)
        if value:
            headers[name] = value
    headers[settings.auth_header] = f"{settings.auth_scheme} {key}".strip()  # set last: always wins
    return headers


def wants_stream(body: bytes) -> bool:
    try:
        payload = json.loads(body)
        return isinstance(payload, dict) and payload.get("stream") is True
    except (ValueError, UnicodeDecodeError):
        return False


async def read_request_body(request: Request, limit: int) -> Optional[bytes]:
    """Read the client body, aborting as soon as it exceeds `limit` (None = too large)."""
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > limit:
        return None
    buf = bytearray()
    async for chunk in request.stream():
        buf += chunk
        if len(buf) > limit:
            return None
    return bytes(buf)


async def read_capped(upstream: httpx.Response, limit: int) -> Optional[bytes]:
    """Read an upstream body, giving up (None) once it exceeds `limit` bytes."""
    buf = bytearray()
    async for chunk in upstream.aiter_raw():
        buf += chunk
        if len(buf) > limit:
            return None
    return bytes(buf)


# --------------------------------------------------------------------------- #
# Application
# --------------------------------------------------------------------------- #
settings = Settings.from_env()
pool = KeyPool(settings.api_keys, settings.cooldown_min_s, settings.cooldown_max_s)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # One shared client = connection pooling / keep-alive to the upstream provider.
    #   connect/read/write: max SILENCE per operation. A stream that keeps producing
    #                       bytes is not cut off by this; MAX_STREAM_SECONDS bounds it.
    #   pool              : fail fast (-> 503) instead of queueing forever when all
    #                       connections are busy (POOL_TIMEOUT_SECONDS; keep it below the
    #                       clients' own timeout so they see the 503, not a ReadTimeout).
    #   follow_redirects  : off, so the pooled key is never replayed to another host.
    app.state.http = httpx.AsyncClient(
        base_url=settings.upstream_base_url,
        timeout=httpx.Timeout(
            connect=10.0, read=settings.timeout_s, write=settings.timeout_s, pool=settings.pool_timeout_s
        ),
        limits=httpx.Limits(
            max_connections=settings.max_connections,
            max_keepalive_connections=min(50, settings.max_connections),
        ),
        follow_redirects=False,
    )
    log.info("proxy up: %d key(s) -> %s%s", len(settings.api_keys), settings.upstream_base_url, settings.upstream_chat_path)
    try:
        yield
    finally:
        await app.state.http.aclose()


app = FastAPI(title="Rate Limiter & Key Swapper Proxy", lifespan=lifespan)


def _bearer_credential(header: str) -> str:
    """'Bearer abc' / 'bearer abc' / 'BEARER  abc' -> 'abc'; any other scheme -> ''."""
    scheme, _, credential = header.strip().partition(" ")
    return credential.strip() if scheme.lower() == "bearer" else ""


def client_is_authorized(request: Request) -> bool:
    if settings.proxy_auth_token is None:
        return True
    # RFC 7235: the auth *scheme* is case-insensitive; the token is compared exactly.
    supplied = _bearer_credential(request.headers.get("authorization", ""))
    return secrets.compare_digest(supplied.encode(), settings.proxy_auth_token.encode())  # constant-time


def admin_guard(request: Request) -> Optional[JSONResponse]:
    """
    Gate for admin endpoints. Returns an error response to send, or None if allowed.

    Unlike the chat route, this FAILS CLOSED: with no PROXY_AUTH_TOKEN configured
    the admin surface is disabled outright rather than left open, since these
    endpoints change which credentials the proxy spends.
    """
    if settings.proxy_auth_token is None:
        return error_response(403, "admin_disabled", "Admin endpoints are disabled. Set PROXY_AUTH_TOKEN to enable them.")
    if not client_is_authorized(request):
        return error_response(401, "unauthorized", "Missing or invalid admin token.", headers={"WWW-Authenticate": "Bearer"})
    return None


@app.post("/proxy/v1/chat")
async def proxy_chat(request: Request) -> Response:
    if not client_is_authorized(request):
        return error_response(401, "unauthorized", "Missing or invalid proxy token.", headers={"WWW-Authenticate": "Bearer"})

    try:
        body = await read_request_body(request, settings.max_body_bytes)
    except ClientDisconnect:
        return Response(status_code=499)
    if body is None:
        return error_response(413, "payload_too_large", f"Request body exceeds {settings.max_body_bytes} bytes.")

    streaming = wants_stream(body)
    http: httpx.AsyncClient = request.app.state.http
    deadline = time.monotonic() + settings.request_deadline_s  # covers ALL retries combined

    # Keys this request has already burned. Belt-and-braces: bench() already makes
    # them unavailable, but this guarantees the loop terminates (<= len(keys)
    # iterations) even if cool-down semantics are changed later.
    tried: set[str] = set()

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return error_response(504, "deadline_exceeded", "Request deadline exceeded before upstream answered.")

        key = await pool.acquire(exclude=frozenset(tried))

        # ---- Exhaustion fallback: every key is cooling down -> 503 ---------- #
        if key is None:
            wait = await pool.seconds_until_available()
            log.error("all %d keys exhausted; returning 503", len(settings.api_keys))
            return error_response(
                503,
                "all_keys_exhausted",
                "All upstream API keys are rate-limited or out of quota. Try again later.",
                headers={"Retry-After": str(wait if wait is not None else 60)},
                retry_after_seconds=wait,
            )

        tried.add(key)
        upstream_req = http.build_request(
            "POST",
            settings.upstream_chat_path,
            content=body,
            headers=build_upstream_headers(settings, key, request),
        )

        try:
            # stream=True returns as soon as headers arrive, so we can inspect the
            # status BEFORE committing to a response. That is what makes
            # swap-and-retry possible even for SSE/streaming requests.
            # wait_for adds a hard wall-clock cap on top of httpx's per-operation
            # timeouts, so a slow-drip upstream cannot hold the request open.
            upstream = await asyncio.wait_for(http.send(upstream_req, stream=True), timeout=remaining)
        except httpx.PoolTimeout:
            return error_response(503, "proxy_busy", "Proxy connection pool is saturated.", headers={"Retry-After": "1"})
        except (asyncio.TimeoutError, httpx.TransportError) as exc:
            # Neither a timeout nor a network failure says anything about the
            # key's health: don't bench it, don't rotate.
            return upstream_failure(exc)

        status = upstream.status_code

        # ---- Success or generic error: relay -------------------------------- #
        # Other 4xx/5xx (bad request, auth failure, upstream 500...) are the
        # client's / provider's problem; rotating keys wouldn't help. Relay as-is.
        if status < 400 or status not in (402, 403, 429):
            return await _relay(upstream, streaming, deadline)

        # ---- Exhaustion path ------------------------------------------------ #
        if status == 429:
            await upstream.aclose()  # body irrelevant; 429 always means "this key is done"
            await pool.bench(key)
            log.info("retrying with next key (attempt %d)", len(tried) + 1)
            continue

        # 402/403: could be quota/billing (bench) or a plain permission error (relay).
        # Read a bounded amount of the body to tell them apart.
        try:
            err_body = await asyncio.wait_for(
                read_capped(upstream, _ERROR_BODY_LIMIT),
                timeout=max(0.1, deadline - time.monotonic()),
            )
        except (asyncio.TimeoutError, httpx.HTTPError) as exc:
            await upstream.aclose()
            return upstream_failure(exc)

        status, hdrs = upstream.status_code, clean_response_headers(upstream.headers)
        await upstream.aclose()

        if err_body is None:
            return error_response(502, "upstream_error_too_large", "Upstream error body exceeded the inspection limit.")
        if is_key_exhausted(status, err_body):
            await pool.bench(key)  # key leaves rotation for 12-24h
            log.info("retrying with next key (attempt %d)", len(tried) + 1)
            continue
        return Response(content=err_body, status_code=status, headers=hdrs)


async def _stream_body(upstream: httpx.Response) -> AsyncIterator[bytes]:
    """
    Pipe upstream bytes through untouched. Guarantees the upstream connection is
    closed on normal end, client disconnect, upstream failure or MAX_STREAM_SECONDS.
    (Headers are already sent at this point, so a mid-stream failure can only
    truncate the stream; it is logged.)
    """
    started = time.monotonic()
    try:
        async for chunk in upstream.aiter_raw():
            yield chunk
            if time.monotonic() - started > settings.max_stream_s:
                log.warning("stream exceeded MAX_STREAM_SECONDS=%s; closing", settings.max_stream_s)
                break
    except httpx.HTTPError as exc:
        log.warning("upstream stream aborted mid-flight: %s", type(exc).__name__)
    finally:
        await upstream.aclose()


async def _relay(upstream: httpx.Response, streaming: bool, deadline: float) -> Response:
    """Turn an open upstream response into a FastAPI response."""
    headers = clean_response_headers(upstream.headers)
    content_type = upstream.headers.get("content-type", "").lower()
    if streaming or content_type.startswith("text/event-stream"):
        return StreamingResponse(_stream_body(upstream), status_code=upstream.status_code, headers=headers)
    try:
        content = await asyncio.wait_for(
            read_capped(upstream, settings.max_response_bytes),
            timeout=max(0.1, deadline - time.monotonic()),
        )
    except (asyncio.TimeoutError, httpx.HTTPError) as exc:
        return upstream_failure(exc)
    finally:
        await upstream.aclose()
    if content is None:
        return error_response(502, "upstream_response_too_large", "Upstream response exceeded MAX_RESPONSE_BYTES.")
    return Response(content=content, status_code=upstream.status_code, headers=headers)


@app.get("/proxy/health")
async def health(request: Request) -> dict:
    """
    Pool status for monitoring. Unauthenticated callers get counts only; key
    fingerprints and countdowns are included only for a valid admin token.
    """
    snap = await pool.snapshot()
    body = {
        "status": "ok" if snap["active_keys"] > 0 else "degraded",
        "total_keys": snap["total_keys"],
        "active_keys": snap["active_keys"],
    }
    if admin_guard(request) is None:
        body["cooling_down"] = snap["cooling_down"]
    return body


# --------------------------------------------------------------------------- #
# Admin endpoints (token-protected, fail-closed)
# --------------------------------------------------------------------------- #
@app.get("/proxy/keys/benched")
async def list_benched_keys(request: Request) -> Response:
    """View keys stuck in cool-down. Use a returned `key` fingerprint with /reinject."""
    if (denied := admin_guard(request)) is not None:
        return denied
    snap = await pool.snapshot()
    return JSONResponse({
        "total_keys": snap["total_keys"],
        "active_keys": snap["active_keys"],
        "benched_count": len(snap["cooling_down"]),
        "benched": snap["cooling_down"],  # [{"key": fingerprint, "available_in_seconds": n}]
    })


@app.post("/proxy/keys/reinject")
async def reinject_keys(request: Request) -> Response:
    """
    Force benched key(s) back into the round-robin loop right now.
    Body must contain exactly one of: {"key_id": "..."}, {"key": "..."}, {"all": true}.
    """
    if (denied := admin_guard(request)) is not None:
        return denied  # auth is checked BEFORE the body is parsed or validated

    try:
        payload = await request.json()
    except ValueError:
        payload = None
    if not isinstance(payload, dict):
        return error_response(400, "bad_request", "Body must be a JSON object.")

    key_id, raw_key, do_all = payload.get("key_id"), payload.get("key"), payload.get("all", False)
    if not (key_id is None or isinstance(key_id, str)) \
            or not (raw_key is None or isinstance(raw_key, str)) \
            or not isinstance(do_all, bool):
        return error_response(400, "bad_request", "'key_id'/'key' must be strings and 'all' a boolean.")
    if sum([key_id is not None, raw_key is not None, do_all]) != 1:
        return error_response(400, "bad_request", "Provide exactly one of: 'key_id', 'key', or 'all': true.")

    caller = request.client.host if request.client else "unknown"  # audit trail

    if do_all:
        released = await pool.reinject_all()
        log.warning("ADMIN reinject-all by %s released %d key(s)", caller, len(released))
        return JSONResponse({"status": "reinjected", "reinjected": released})

    outcome, fp = await pool.reinject(key_id=key_id, raw_key=raw_key)
    log.warning("ADMIN reinject by %s -> %s (%s)", caller, outcome, fp or "n/a")

    if outcome == "reinjected":
        return JSONResponse({"status": "reinjected", "key": fp})
    if outcome == "not_benched":
        return error_response(409, "not_benched", "That key is already active; nothing to reinject.", key=fp)
    if outcome == "ambiguous":
        return error_response(409, "ambiguous_key_id", "Fingerprint matches multiple keys; send the exact key instead.")
    return error_response(404, "unknown_key", "No key in the pool matches that identifier.")


if __name__ == "__main__":
    import uvicorn

    # Single worker on purpose: pool state is per-process (see module docstring).
    uvicorn.run(app, host=os.getenv("HOST", "127.0.0.1"), port=int(os.getenv("PORT", "8000")), workers=1)
