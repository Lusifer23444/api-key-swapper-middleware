"""Optional HTTP wrapper: run keyswap as a small standalone proxy for any language.

    pip install "keyswap[proxy]"
    export API_KEYS="key1,key2"            # or a .env file, or add keys later over HTTP
    export PROXY_AUTH_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
    python -m keyswap

    client --POST /proxy/v1/chat--> [keyswap] --> provider        (same JSON body as the provider's API)

Endpoints
    POST   /proxy/v1/chat                    the proxied call ("stream": true is supported)
    GET    /proxy/health                     counts only; per-key states when the admin token is sent
  Admin (need "Authorization: Bearer $PROXY_AUTH_TOKEN"; disabled if no token is configured)
    GET    /proxy/keys                       ids and states (never secrets)
    POST   /proxy/keys                       {"key": "...", "key_id": "optional-name"}  add at runtime
    DELETE /proxy/keys/{key_id}              remove (drains in-flight requests first)
    POST   /proxy/keys/{key_id}/reinstate    force a cooling/disabled key back to active

Notes
    * State is per process: run ONE worker. Keys added over HTTP live in memory only.
    * Upstream credentials never come from the client; client headers are allowlisted.
"""

from __future__ import annotations

import json
import logging
import math
import os
import secrets
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Mapping, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.background import BackgroundTask
from starlette.requests import ClientDisconnect

from .errors import (
    AllKeysUnavailable, DeadlineExceeded, GatewayBusy, KeySwapError, ModelUnavailable,
    ProviderUnavailable, ResponseTooLarge,
)
from .gateway import Gateway, GatewayConfig, Provider
from .models import ModelMap
from .sources import keys_from_env, read_dotenv
from .store import JsonStateStore

log = logging.getLogger("keyswap")

# Hop-by-hop / recomputed headers, plus ones that would leak the upstream account's identity.
_STRIP_RESPONSE_HEADERS = {
    "connection", "keep-alive", "transfer-encoding", "content-length", "content-encoding", "te",
    "trailer", "upgrade", "proxy-authenticate", "set-cookie", "set-cookie2",
    "openai-organization", "openai-project", "anthropic-organization-id",
}


def clean_response_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {k: v for k, v in headers.items() if k.lower() not in _STRIP_RESPONSE_HEADERS}


def error_response(status: int, err_type: str, message: str, headers: Optional[dict] = None, **extra) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"error": {"type": err_type, "message": message, **extra}},
        headers=headers,
    )


def _bearer(header: str) -> str:
    scheme, _, credential = header.strip().partition(" ")
    return credential.strip() if scheme.lower() == "bearer" else ""


def _map_error(exc: KeySwapError) -> JSONResponse:
    """Translate a keyswap error into the right HTTP answer. 429-ish and 503-ish stay distinguishable."""
    if isinstance(exc, AllKeysUnavailable):
        wait = exc.retry_after
        return error_response(
            503, "all_keys_exhausted" if wait is not None else "no_usable_keys", str(exc),
            headers={"Retry-After": str(max(1, math.ceil(wait)) if wait is not None else 60)},
            retry_after_seconds=wait,
        )
    if isinstance(exc, ProviderUnavailable):
        return error_response(503, "provider_unavailable", str(exc), headers={"Retry-After": "5"})
    if isinstance(exc, GatewayBusy):
        return error_response(503, "proxy_busy", str(exc), headers={"Retry-After": "1"})
    if isinstance(exc, ModelUnavailable):
        return error_response(502, "model_unavailable", str(exc))
    if isinstance(exc, DeadlineExceeded):
        return error_response(504, "deadline_exceeded", str(exc))
    if isinstance(exc, ResponseTooLarge):
        return error_response(502, "upstream_response_too_large", str(exc))
    return error_response(500, "internal_error", str(exc))


async def _read_body(request: Request, limit: int) -> Optional[bytes]:
    """Read the client body, stopping as soon as it exceeds `limit` (None = too large)."""
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > limit:
        return None
    buf = bytearray()
    async for chunk in request.stream():
        buf += chunk
        if len(buf) > limit:
            return None
    return bytes(buf)


def create_app(
    gateway: Gateway,
    *,
    auth_token: Optional[str] = None,
    allow_unauthenticated: bool = False,
    max_body_bytes: int = 10 * 1024 * 1024,
) -> FastAPI:
    if auth_token is None and not allow_unauthenticated:
        raise ValueError(
            "auth_token is required (set PROXY_AUTH_TOKEN when running `python -m keyswap`): without it "
            "anyone who can reach this port can spend your API keys. "
            "Pass allow_unauthenticated=True for local development only."
        )
    if auth_token is None:
        log.warning("running WITHOUT auth: /proxy/v1/chat is open and admin endpoints are disabled")
    elif len(auth_token) < 16:
        log.warning("auth token is shorter than 16 characters; use a long random value")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        await gateway.aclose()

    app = FastAPI(title="keyswap proxy", lifespan=lifespan)
    app.state.gateway = gateway

    def authorized(request: Request) -> bool:
        if auth_token is None:
            return True
        supplied = _bearer(request.headers.get("authorization", ""))
        return secrets.compare_digest(supplied.encode(), auth_token.encode())  # constant-time

    def admin_guard(request: Request) -> Optional[JSONResponse]:
        """Admin fails CLOSED: with no token configured the admin surface is off, not open."""
        if auth_token is None:
            return error_response(403, "admin_disabled", "Admin endpoints are disabled. Configure an auth token.")
        if not authorized(request):
            return error_response(401, "unauthorized", "Missing or invalid admin token.",
                                  headers={"WWW-Authenticate": "Bearer"})
        return None

    # ------------------------------------------------------------------ #
    @app.post("/proxy/v1/chat")
    async def chat(request: Request) -> Response:
        if not authorized(request):
            return error_response(401, "unauthorized", "Missing or invalid proxy token.",
                                  headers={"WWW-Authenticate": "Bearer"})
        try:
            raw = await _read_body(request, max_body_bytes)
        except ClientDisconnect:
            return Response(status_code=499)
        if raw is None:
            return error_response(413, "payload_too_large", f"Request body exceeds {max_body_bytes} bytes.")
        try:
            payload = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            payload = None
        if not isinstance(payload, dict):
            return error_response(400, "bad_request", "Body must be a JSON object.")

        client_headers = dict(request.headers)
        if payload.get("stream") is not True:
            try:
                reply = await gateway.chat(payload, headers=client_headers)
            except KeySwapError as exc:
                return _map_error(exc)
            return Response(content=reply.content, status_code=reply.status_code,
                            headers=clean_response_headers(reply.headers))

        # Streaming: open the stream (all key swapping happens here, before any byte is sent),
        # then relay chunks. The context is closed when the stream ends or the client leaves.
        cm = gateway.stream(payload, headers=client_headers)
        try:
            stream = await cm.__aenter__()
        except KeySwapError as exc:
            return _map_error(exc)

        closed = False

        async def close() -> None:
            nonlocal closed
            if not closed:
                closed = True
                await cm.__aexit__(None, None, None)

        async def body():
            try:
                async for chunk in stream.aiter_bytes():
                    yield chunk
            finally:
                await close()

        return StreamingResponse(body(), status_code=stream.status_code,
                                 headers=clean_response_headers(stream.headers),
                                 background=BackgroundTask(close))

    @app.get("/proxy/health")
    async def health(request: Request) -> dict:
        snap = gateway.status()
        active = sum(1 for k in snap if k["state"] == "active")
        body = {"status": "ok" if active else "degraded", "total_keys": len(snap), "active_keys": active}
        if admin_guard(request) is None:
            body["keys"] = snap
        return body

    @app.get("/proxy/keys")
    async def list_keys(request: Request) -> Response:
        if (denied := admin_guard(request)) is not None:
            return denied
        return JSONResponse({"keys": gateway.status()})

    @app.post("/proxy/keys")
    async def add_key(request: Request) -> Response:
        if (denied := admin_guard(request)) is not None:
            return denied  # auth is checked BEFORE the body is parsed
        try:
            data = await request.json()
        except ValueError:
            data = None
        if not isinstance(data, dict) or not isinstance(data.get("key"), str):
            return error_response(400, "bad_request", 'Body must be {"key": "...", "key_id": "optional"}.')
        key_id = data.get("key_id")
        if key_id is not None and not isinstance(key_id, str):
            return error_response(400, "bad_request", "'key_id' must be a string.")
        try:
            new_id = gateway.add_key(data["key"], key_id)
        except ValueError as exc:
            status = 409 if "already" in str(exc) else 400
            return error_response(status, "invalid_key", str(exc))
        log.warning("ADMIN added key %s via HTTP (%s)", new_id, request.client.host if request.client else "?")
        return JSONResponse({"key_id": new_id}, status_code=201)

    @app.delete("/proxy/keys/{key_id}")
    async def remove_key(key_id: str, request: Request) -> Response:
        if (denied := admin_guard(request)) is not None:
            return denied
        if not gateway.remove_key(key_id):
            return error_response(404, "unknown_key", "No key with that id.")
        log.warning("ADMIN removed key %s via HTTP", key_id)
        return JSONResponse({"status": "removed", "key_id": key_id})

    @app.post("/proxy/keys/{key_id}/reinstate")
    async def reinstate_key(key_id: str, request: Request) -> Response:
        if (denied := admin_guard(request)) is not None:
            return denied
        if not gateway.pool.reinstate(key_id):
            return error_response(404, "unknown_key", "No key with that id.")
        return JSONResponse({"status": "reinstated", "key_id": key_id})

    return app


# ---------------------------------------------------------------------- #
# Configuration from environment / .env (used by `python -m keyswap`)
# ---------------------------------------------------------------------- #
def _truthy(value: Optional[str]) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _number(env: Mapping[str, str], name: str, default, cast):
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = cast(raw)
    except ValueError:
        raise RuntimeError(f"{name} must be a number, got {raw!r}") from None
    if value <= 0:
        raise RuntimeError(f"{name} must be > 0")
    return value


def load_env(environ: Optional[Mapping[str, str]] = None, dotenv_path: str = ".env") -> dict[str, str]:
    """Real environment variables, with a .env file filling in what is missing (unless DISABLE_DOTENV)."""
    real = dict(environ if environ is not None else os.environ)
    if _truthy(real.get("DISABLE_DOTENV")):
        return real
    return {**read_dotenv(dotenv_path), **real}


def app_from_env(environ: Optional[Mapping[str, str]] = None, dotenv_path: str = ".env") -> FastAPI:
    env = load_env(environ, dotenv_path)

    name = env.get("KEYSWAP_PROVIDER", "openai").strip().lower()
    presets = {"openai": Provider.openai, "anthropic": Provider.anthropic, "gemini": Provider.gemini_openai_compat}
    if name == "custom":
        if not env.get("UPSTREAM_BASE_URL"):
            raise RuntimeError("KEYSWAP_PROVIDER=custom needs UPSTREAM_BASE_URL")
        try:
            provider = Provider(env["UPSTREAM_BASE_URL"].strip())
        except ValueError as exc:  # same loud, uniform startup error as every other bad setting
            raise RuntimeError(str(exc)) from None
    elif name in presets:
        provider = presets[name]()
    else:
        raise RuntimeError(f"KEYSWAP_PROVIDER must be one of: {', '.join(sorted(presets))}, custom")

    overrides: dict = {}
    if env.get("UPSTREAM_BASE_URL"):
        overrides["base_url"] = env["UPSTREAM_BASE_URL"].strip().rstrip("/")
    if env.get("UPSTREAM_CHAT_PATH"):
        overrides["chat_path"] = env["UPSTREAM_CHAT_PATH"].strip()
    if env.get("UPSTREAM_AUTH_HEADER"):
        overrides["auth_header"] = env["UPSTREAM_AUTH_HEADER"].strip()
    if env.get("UPSTREAM_AUTH_SCHEME") is not None:
        overrides["auth_scheme"] = env["UPSTREAM_AUTH_SCHEME"]
    if env.get("FORWARD_HEADERS") is not None:
        overrides["forward_headers"] = tuple(h for h in env["FORWARD_HEADERS"].split(",") if h.strip())
    if _truthy(env.get("ALLOW_INSECURE_UPSTREAM")):
        overrides["allow_insecure"] = True
    if overrides:
        try:
            provider = replace(provider, **overrides)
        except ValueError as exc:
            raise RuntimeError(str(exc)) from None

    aliases: dict = {}
    if env.get("MODEL_ALIASES"):
        try:
            aliases = json.loads(env["MODEL_ALIASES"])
            ok = isinstance(aliases, dict) and all(isinstance(v, list) and v for v in aliases.values())
        except ValueError:
            ok = False
        if not ok:
            raise RuntimeError('MODEL_ALIASES must be JSON like {"fast": ["model-a", "model-b"]}')

    cfg = GatewayConfig(
        request_deadline_s=_number(env, "REQUEST_DEADLINE_SECONDS", 120.0, float),
        attempt_timeout_s=_number(env, "UPSTREAM_TIMEOUT_SECONDS", 60.0, float),
        max_stream_s=_number(env, "MAX_STREAM_SECONDS", 900.0, float),
        max_response_bytes=_number(env, "MAX_RESPONSE_BYTES", 32 * 1024 * 1024, int),
        max_connections=_number(env, "MAX_CONNECTIONS", 200, int),
        pool_timeout_s=_number(env, "POOL_TIMEOUT_SECONDS", 5.0, float),
    )
    state_file = env.get("STATE_FILE")
    gateway = Gateway(provider, models=ModelMap(aliases), config=cfg,
                      store=JsonStateStore(state_file) if state_file else None)

    keys = keys_from_env("API_KEYS", environ=env)
    ids = gateway.add_keys(keys, skip_duplicates=True)
    token = env.get("PROXY_AUTH_TOKEN") or None
    if not ids:
        log.warning("no keys loaded at startup; add them with POST /proxy/keys (needs the admin token)")
    log.info("keyswap proxy: provider=%s, %d key(s) loaded", name, len(ids))

    return create_app(
        gateway,
        auth_token=token,
        allow_unauthenticated=_truthy(env.get("ALLOW_UNAUTHENTICATED")),
        max_body_bytes=_number(env, "MAX_REQUEST_BODY_BYTES", 10 * 1024 * 1024, int),
    )
