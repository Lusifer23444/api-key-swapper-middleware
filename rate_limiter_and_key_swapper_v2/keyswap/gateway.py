"""Gateway: sends one chat request, swapping keys and models when the provider pushes back.

Behaviour per upstream answer (see classify.py):
    2xx                -> done
    429 (burst)        -> cool down THAT key (Retry-After if given, else backoff), try the next key
    429/402 (quota)    -> bench THAT key for hours, try the next key
    401 / bad key      -> DISABLE that key, try the next key
    model not found    -> if you used an alias: skip that model, try the next one in the chain
    502/503/504/529    -> provider problem: back off the PROVIDER, retry a couple of times,
                          never penalise keys
    anything else      -> returned to you as-is (it is your request's problem, not ours)

Two ways to call it:
    reply = await gw.chat(payload)                  # whole answer at once
    async with gw.stream(payload) as s:             # streaming (SSE); keys are swapped BEFORE
        async for chunk in s.aiter_bytes(): ...     # the first byte is sent, so a 429 never
                                                    # breaks a stream halfway

One provider per Gateway.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import AsyncIterator, Awaitable, Callable, Mapping, Optional
from urllib.parse import urlsplit

import httpx

from .classify import Kind, classify
from .errors import (
    AllKeysUnavailable, DeadlineExceeded, GatewayBusy, ModelUnavailable, ProviderUnavailable,
    ResponseTooLarge,
)
from .models import ModelMap
from .pool import KeyPool, Lease, Outcome
from .sources import keys_from_env, keys_from_file
from .store import JsonStateStore

log = logging.getLogger("keyswap")

_HEADER_TOKEN_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")  # RFC 9110 header-name "token"
_ERROR_BODY_LIMIT = 64 * 1024  # error bodies are only read far enough to classify them

# Client headers that may never be forwarded upstream, whatever the allowlist says.
_NEVER_FORWARD = frozenset({
    "authorization", "proxy-authorization", "cookie", "host", "content-length",
    "x-api-key", "api-key", "x-goog-api-key",
})


@dataclass(frozen=True)
class Provider:
    """Where and how to call one provider. Only this part differs between providers."""
    base_url: str
    chat_path: str = "/v1/chat/completions"
    auth_header: str = "Authorization"
    auth_scheme: str = "Bearer"                 # set "" to send the raw key
    extra_headers: dict = field(default_factory=dict)   # always sent (e.g. anthropic-version)
    forward_headers: tuple = ()                 # client headers allowed through (allowlist)
    allow_insecure: bool = False                # plain http:// to a non-local host, off by default

    def __post_init__(self) -> None:
        parts = urlsplit(self.base_url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise ValueError("Provider.base_url must be an absolute http(s) URL.")
        is_local = parts.hostname in {"localhost", "127.0.0.1", "::1"}
        if parts.scheme == "http" and not is_local and not self.allow_insecure:
            raise ValueError(
                "Provider.base_url uses plain http:// for a non-local host, which would send your API "
                "keys in cleartext. Use https://, or pass allow_insecure=True if you really mean it."
            )
        if not _HEADER_TOKEN_RE.match(self.auth_header):
            raise ValueError("Provider.auth_header is not a valid HTTP header name.")
        forward = tuple(h.strip().lower() for h in self.forward_headers if h.strip())
        for h in forward:
            if not _HEADER_TOKEN_RE.match(h):
                raise ValueError(f"forward_headers contains an invalid header name: {h!r}")
        forbidden = set(forward) & (_NEVER_FORWARD | {self.auth_header.lower()})
        if forbidden:
            raise ValueError(f"forward_headers may not include credential/hop headers: {sorted(forbidden)}")
        object.__setattr__(self, "forward_headers", forward)

    @classmethod
    def openai(cls) -> "Provider":
        return cls("https://api.openai.com", forward_headers=("openai-beta",))

    @classmethod
    def anthropic(cls) -> "Provider":
        return cls("https://api.anthropic.com", "/v1/messages", "x-api-key", "",
                   {"anthropic-version": "2023-06-01"},
                   forward_headers=("anthropic-version", "anthropic-beta"))

    @classmethod
    def gemini_openai_compat(cls) -> "Provider":
        # Google's OpenAI-compatible endpoint. Check Google's current docs; paths do change.
        return cls("https://generativelanguage.googleapis.com/v1beta/openai", "/chat/completions")


@dataclass(frozen=True)
class GatewayConfig:
    request_deadline_s: float = 120.0     # total budget for one call, all retries included
    attempt_timeout_s: float = 60.0       # waiting for response headers / silence on one attempt
    max_provider_retries: int = 2         # extra tries after 502/503/504/529 or network errors
    provider_backoff_base_s: float = 1.0
    provider_backoff_max_s: float = 30.0
    max_stream_s: float = 900.0           # hard ceiling for one streamed response
    max_response_bytes: int = 32 * 1024 * 1024  # non-streaming responses only
    max_connections: int = 200
    pool_timeout_s: float = 5.0           # wait for a free connection, then GatewayBusy


@dataclass
class GatewayResponse:
    status_code: int
    headers: dict
    content: bytes
    key_id: str
    model: Optional[str]
    attempts: int

    @property
    def ok(self) -> bool:
        return self.status_code < 400

    def json(self):
        return json.loads(self.content)


class GatewayStream:
    """An open streaming answer. Use through `async with gw.stream(...) as s`."""

    def __init__(self, *, status_code: int, headers: dict, key_id: str, model: Optional[str], attempts: int,
                 response: Optional[httpx.Response], lease: Optional[Lease], pool: KeyPool,
                 buffer: Optional[bytes], max_seconds: float) -> None:
        self.status_code = status_code
        self.headers = headers
        self.key_id = key_id
        self.model = model
        self.attempts = attempts
        self.failed = False        # upstream broke mid-stream
        self.truncated = False     # cut off by max_stream_s
        self._response = response
        self._lease = lease
        self._pool = pool
        self._buffer = buffer      # set for non-2xx answers: the whole (small) body, already read
        self._max_seconds = max_seconds
        self._closed = False

    @property
    def ok(self) -> bool:
        return self.status_code < 400

    async def aiter_bytes(self) -> AsyncIterator[bytes]:
        if self._buffer is not None:
            if self._buffer:
                yield self._buffer
            return
        started = time.monotonic()
        try:
            try:
                whole = self._response.content  # type: ignore[union-attr]
            except httpx.ResponseNotRead:
                whole = None
            if whole is not None:  # fully-read response (custom transports): nothing to iterate
                if whole:
                    yield whole
                return
            async for chunk in self._response.aiter_raw():  # type: ignore[union-attr]
                yield chunk
                if time.monotonic() - started > self._max_seconds:
                    log.warning("stream exceeded max_stream_s=%s; closing", self._max_seconds)
                    self.truncated = True
                    break
        except httpx.HTTPError as exc:
            # Headers are already sent, so all we can do is stop. The key did nothing wrong.
            self.failed = True
            log.warning("upstream stream aborted mid-flight: %s", type(exc).__name__)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self._response is not None:
                await self._response.aclose()
        finally:
            if self._lease is not None:
                self._pool.release(self._lease, Outcome.NEUTRAL if self.failed else Outcome.SUCCESS)


@dataclass
class _Run:
    """Internal result of the retry loop."""
    response: httpx.Response
    content: Optional[bytes]       # None only for an open stream
    lease: Lease
    model: Optional[str]
    attempts: int
    lease_held: bool               # True: caller now owns the lease (open stream)


class Gateway:
    def __init__(
        self,
        provider: Provider,
        pool: Optional[KeyPool] = None,
        models: Optional[ModelMap] = None,
        *,
        config: Optional[GatewayConfig] = None,
        client: Optional[httpx.AsyncClient] = None,
        store: Optional[JsonStateStore] = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
        rng: Optional[random.Random] = None,
    ) -> None:
        self.provider = provider
        self.pool = pool or KeyPool()
        self.models = models or ModelMap()
        self.config = config or GatewayConfig()
        self._client = client
        self._owns_client = client is None
        self._sleep = sleep
        self._clock = clock
        self._rng = rng or random.Random()
        # Provider-level breaker: shared by all requests, independent of any key.
        self._provider_failures = 0
        self._provider_open_until = 0.0
        if store is not None:
            self.pool.load_state(store.load())
            self.pool.on_change = lambda: store.save(self.pool.export_state())

    # -- key management shortcuts (every one of these is optional) --------- #
    def add_key(self, secret: str, key_id: Optional[str] = None) -> str:
        return self.pool.add_key(secret, key_id)

    def add_keys(self, secrets_, *, skip_duplicates: bool = False) -> list[str]:
        return self.pool.add_keys(secrets_, skip_duplicates=skip_duplicates)

    def add_keys_from_env(self, name: str = "API_KEYS", dotenv: Optional[str] = ".env") -> list[str]:
        """Register keys from an env var (comma separated, or NAME_1, NAME_2 ...) and/or a .env file."""
        return self.pool.add_keys(keys_from_env(name, dotenv_path=dotenv), skip_duplicates=True)

    def add_keys_from_file(self, path: str) -> list[str]:
        return self.pool.add_keys(keys_from_file(path), skip_duplicates=True)

    def remove_key(self, key_id: str) -> bool:
        return self.pool.remove_key(key_id)

    def status(self) -> list[dict]:
        return self.pool.snapshot()

    async def aclose(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "Gateway":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()

    # ------------------------------------------------------------------ #
    # Public calls
    # ------------------------------------------------------------------ #
    async def chat(self, payload: dict, *, headers: Optional[Mapping[str, str]] = None) -> GatewayResponse:
        """Send one request and return the whole answer, or raise a KeySwapError subclass."""
        run = await self._run(payload, headers, want_stream=False)
        return GatewayResponse(
            status_code=run.response.status_code,
            headers=dict(run.response.headers),
            content=run.content or b"",
            key_id=run.lease.key_id,
            model=run.model,
            attempts=run.attempts,
        )

    @asynccontextmanager
    async def stream(
        self, payload: dict, *, headers: Optional[Mapping[str, str]] = None
    ) -> AsyncIterator[GatewayStream]:
        """Send a request with `"stream": true` and read the answer as it arrives.

        All key swapping happens before the first byte is yielded. A non-2xx answer arrives
        as a small buffered body (the same iteration API), so callers can relay it uniformly.
        """
        run = await self._run(payload, headers, want_stream=True)
        s = GatewayStream(
            status_code=run.response.status_code,
            headers=dict(run.response.headers),
            key_id=run.lease.key_id,
            model=run.model,
            attempts=run.attempts,
            response=run.response if run.lease_held else None,
            lease=run.lease if run.lease_held else None,
            pool=self.pool,
            buffer=run.content,
            max_seconds=self.config.max_stream_s,
        )
        try:
            yield s
        finally:
            await s.aclose()

    # ------------------------------------------------------------------ #
    # The retry loop (shared by chat and stream)
    # ------------------------------------------------------------------ #
    async def _run(self, payload: dict, client_headers: Optional[Mapping[str, str]], want_stream: bool) -> _Run:
        if not isinstance(payload, dict):
            raise TypeError("payload must be a dict")
        cfg = self.config
        deadline = self._clock() + cfg.request_deadline_s
        requested = payload.get("model")
        tried: set[str] = set()
        provider_retries = 0
        attempts = 0

        while True:
            await self._wait_out_provider_backoff(deadline)

            model = None
            if requested is not None:
                model = self.models.resolve(requested)
                if model is None:
                    raise ModelUnavailable(requested)

            lease = self.pool.acquire(exclude=tried)
            if lease is None:
                raise AllKeysUnavailable(self.pool.next_available_in(), self.pool.snapshot())

            attempts += 1
            body = dict(payload)
            if model is not None:
                body["model"] = model

            outcome, retry_after = Outcome.NEUTRAL, None
            response: Optional[httpx.Response] = None
            content: Optional[bytes] = None
            verdict = None
            keep_lease = False
            try:
                remaining = deadline - self._clock()
                if remaining <= 0:
                    raise DeadlineExceeded("Request deadline exceeded before upstream answered.")
                try:
                    response, content = await asyncio.wait_for(
                        self._open(lease, body, client_headers, want_stream),
                        timeout=min(remaining, cfg.attempt_timeout_s),
                    )
                except httpx.PoolTimeout:
                    raise GatewayBusy() from None
                except (httpx.HTTPError, asyncio.TimeoutError) as exc:
                    log.warning("network problem talking to provider: %s", type(exc).__name__)
                else:
                    verdict = classify(response.status_code, content or b"", response.headers)
                    retry_after = verdict.retry_after
                    outcome = {
                        Kind.OK: Outcome.SUCCESS,
                        Kind.RATE_LIMITED: Outcome.RATE_LIMITED,
                        Kind.QUOTA_EXHAUSTED: Outcome.QUOTA_EXHAUSTED,
                        Kind.AUTH_FAILED: Outcome.AUTH_FAILED,
                    }.get(verdict.kind, Outcome.NEUTRAL)
                    if want_stream and verdict.kind is Kind.OK:
                        # Key works. Keep the lease (the stream is still using the key) but record
                        # the success now so a probing key recovers immediately.
                        self.pool.confirm_success(lease)
                        keep_lease = True
            finally:
                if not keep_lease:  # always hand the key back, even on cancellation or errors
                    self.pool.release(lease, outcome, retry_after if outcome is Outcome.RATE_LIMITED else None)

            # ---- network failure (no response at all) ------------------------------- #
            if response is None:
                provider_retries += 1
                self._record_provider_failure()
                if provider_retries > cfg.max_provider_retries:
                    raise ProviderUnavailable(None)
                continue

            kind = verdict.kind

            if kind is Kind.OK:
                self._record_provider_success()
                return _Run(response, content, lease, model, attempts, lease_held=keep_lease)

            if kind is Kind.PASSTHROUGH:
                return _Run(response, content, lease, model, attempts, lease_held=False)

            if kind in (Kind.RATE_LIMITED, Kind.QUOTA_EXHAUSTED, Kind.AUTH_FAILED):
                tried.add(lease.key_id)  # this key can't help this request; try another
                continue

            if kind is Kind.MODEL_NOT_FOUND:
                if requested is not None and self.models.is_alias(requested):
                    self.models.mark_unavailable(model)
                    continue  # the key did nothing wrong; resolve() now returns the next model
                return _Run(response, content, lease, model, attempts, lease_held=False)  # raw name: caller's mistake

            # Kind.PROVIDER_ERROR: the provider is struggling. Not the key's fault.
            provider_retries += 1
            self._record_provider_failure(verdict.retry_after)
            if provider_retries > cfg.max_provider_retries:
                raise ProviderUnavailable(response.status_code)

    # ------------------------------------------------------------------ #
    # One HTTP attempt
    # ------------------------------------------------------------------ #
    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            cfg = self.config
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(connect=10.0, read=cfg.attempt_timeout_s,
                                      write=cfg.attempt_timeout_s, pool=cfg.pool_timeout_s),
                limits=httpx.Limits(max_connections=cfg.max_connections,
                                    max_keepalive_connections=min(50, cfg.max_connections)),
                follow_redirects=False,  # a redirect would replay the key to another host
            )
        return self._client

    def _build_headers(self, lease: Lease, client_headers: Optional[Mapping[str, str]]) -> dict[str, str]:
        """Allowlist, never copy wholesale: the caller's own token, cookies etc. cannot leak upstream."""
        p = self.provider
        low = {k.lower(): v for k, v in (client_headers or {}).items()}
        headers = {
            "Content-Type": "application/json",
            "Accept": low.get("accept", "application/json"),
            "Accept-Encoding": "identity",  # keep bytes as-is: no decompression needed for streaming/inspection
        }
        headers.update(p.extra_headers)
        for name in p.forward_headers:
            if low.get(name):
                headers[name] = low[name]
        headers[p.auth_header] = f"{p.auth_scheme} {lease.secret.reveal()}".strip()  # set last: always wins
        return headers

    async def _open(self, lease: Lease, body: dict, client_headers, want_stream: bool):
        """Send one request. Returns (response, content); content is None for an open stream."""
        client = self._ensure_client()
        p = self.provider
        url = p.base_url.rstrip("/") + "/" + p.chat_path.lstrip("/")
        request = client.build_request("POST", url, json=body, headers=self._build_headers(lease, client_headers))
        # stream=True returns as soon as headers arrive, so the status can be judged BEFORE any
        # body is consumed. That is what makes swap-and-retry possible even for SSE.
        response = await client.send(request, stream=True)
        try:
            if response.status_code < 400 and want_stream:
                return response, None
            if response.status_code >= 400:
                content, _ = await _read_capped(response, _ERROR_BODY_LIMIT, truncate=True)
            else:
                content, _ = await _read_capped(response, self.config.max_response_bytes, truncate=False)
        except BaseException:
            await response.aclose()
            raise
        await response.aclose()
        return response, content

    # -- provider-level backoff (separate from key health on purpose) ------ #
    def _record_provider_failure(self, retry_after: Optional[float] = None) -> None:
        cfg = self.config
        self._provider_failures += 1
        raw = min(cfg.provider_backoff_base_s * (2 ** min(self._provider_failures - 1, 30)),
                  cfg.provider_backoff_max_s)
        delay = raw / 2 + self._rng.uniform(0, raw / 2)
        if retry_after is not None:
            delay = max(delay, min(retry_after, cfg.provider_backoff_max_s))
        self._provider_open_until = max(self._provider_open_until, self._clock() + delay)
        log.warning("provider trouble (#%d in a row); backing off %.1fs; keys are not penalised",
                    self._provider_failures, delay)

    def _record_provider_success(self) -> None:
        self._provider_failures = 0
        self._provider_open_until = 0.0

    async def _wait_out_provider_backoff(self, deadline: float) -> None:
        wait = self._provider_open_until - self._clock()
        if wait <= 0:
            return
        if wait >= deadline - self._clock():
            raise DeadlineExceeded("Provider is backing off for longer than the request deadline allows.")
        await self._sleep(wait)


async def _read_capped(response: httpx.Response, limit: int, *, truncate: bool) -> tuple[bytes, bool]:
    """Read a body up to `limit` bytes. Over the limit: truncate (error bodies) or raise (answers)."""
    try:
        data = response.content  # already in memory (some transports deliver fully-read responses)
    except httpx.ResponseNotRead:
        data = None
    if data is not None:
        if len(data) > limit:
            if truncate:
                return data[:limit], True
            raise ResponseTooLarge(f"Provider response exceeded {limit} bytes.")
        return data, False
    buf = bytearray()
    async for chunk in response.aiter_raw():
        buf += chunk
        if len(buf) > limit:
            if truncate:
                return bytes(buf[:limit]), True
            raise ResponseTooLarge(f"Provider response exceeded {limit} bytes.")
    return bytes(buf), False
