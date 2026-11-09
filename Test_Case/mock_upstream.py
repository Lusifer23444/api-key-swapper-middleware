"""
mock_upstream.py -- a fake "AI provider" for testing the key-swapper proxy offline.

    uvicorn mock_upstream:app --port 9001

Behaviour is chosen PER API KEY (the key the proxy sent in `Authorization: Bearer <key>`),
so a test can say "k1 is rate-limited, k2 is healthy" without burning real quota.

Modes
-----
    ok            200 JSON (or SSE if the request had "stream": true)
    429           rate limit error (with Retry-After)
    quota403      403 whose body says insufficient_quota   -> proxy should bench the key
    forbidden403  403 with NO quota wording                -> proxy should pass it through
    400 / 500     plain errors                             -> proxy should pass them through
    stall         sleep `stall_seconds` BEFORE sending headers (simulates a hung provider)
    drip          200, but the body trickles out 1 byte / 0.3 s (simulates a slow-loris upstream)

Control API (used by test_proxy.py, handy with curl too)
--------------------------------------------------------
    POST /_control/config  {"modes": {"k1": "429"}, "default": "ok", "stall_seconds": 20}
    GET  /_control/log     every chat request received: key, mode, stream flag, ALL headers
    POST /_control/reset   clear modes and log
"""

from __future__ import annotations

import asyncio
import json
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI(title="Mock AI upstream")

STATE: dict = {"modes": {}, "default": "ok", "stall_seconds": 30.0, "log": []}

# Headers a real provider might send that the proxy must NOT forward to its clients.
LEAKY_HEADERS = {
    "Set-Cookie": "session=upstream-secret; HttpOnly",
    "openai-organization": "org-secret-123",
    "openai-project": "proj-secret-456",
    "x-request-id": "req-abc",  # harmless: SHOULD pass through
}


@app.post("/_control/config")
async def control_config(request: Request) -> dict:
    cfg = await request.json()
    if "modes" in cfg:
        STATE["modes"].update(cfg["modes"])
    if "default" in cfg:
        STATE["default"] = cfg["default"]
    if "stall_seconds" in cfg:
        STATE["stall_seconds"] = float(cfg["stall_seconds"])
    return {"ok": True, "modes": STATE["modes"], "default": STATE["default"]}


@app.post("/_control/reset")
async def control_reset() -> dict:
    STATE["modes"].clear()
    STATE["default"] = "ok"
    STATE["stall_seconds"] = 30.0
    STATE["log"].clear()
    return {"ok": True}


@app.get("/_control/log")
async def control_log() -> list:
    return STATE["log"]


@app.get("/_control/ping")
async def ping() -> dict:
    return {"pong": True}


async def _sse() -> AsyncIterator[bytes]:
    for i in range(3):
        yield f'data: {{"chunk": {i}}}\n\n'.encode()
        await asyncio.sleep(0.02)
    yield b"data: [DONE]\n\n"


async def _drip() -> AsyncIterator[bytes]:
    for _ in range(200):
        yield b" "
        await asyncio.sleep(0.3)


@app.post("/v1/chat/completions")
async def chat(request: Request):
    auth = request.headers.get("authorization", "")
    key = auth[len("Bearer "):].strip() if auth.startswith("Bearer ") else auth
    raw = await request.body()
    try:
        payload = json.loads(raw)
    except ValueError:
        payload = {}
    streaming = isinstance(payload, dict) and payload.get("stream") is True

    mode = STATE["modes"].get(key, STATE["default"])
    STATE["log"].append({"key": key, "mode": mode, "stream": streaming, "headers": dict(request.headers)})

    if mode == "429":
        return JSONResponse(
            {"error": {"type": "rate_limit_error", "message": "Rate limit reached for requests"}},
            status_code=429, headers={"Retry-After": "20"},
        )
    if mode == "quota403":
        return JSONResponse(
            {"error": {"code": "insufficient_quota", "message": "You exceeded your current quota."}},
            status_code=403,
        )
    if mode == "forbidden403":
        return JSONResponse({"error": {"message": "Invalid permissions for this model."}}, status_code=403)
    if mode == "400":
        return JSONResponse({"error": {"message": "bad request"}}, status_code=400)
    if mode == "500":
        return JSONResponse({"error": {"message": "upstream exploded"}}, status_code=500)
    if mode == "drip":
        return StreamingResponse(_drip(), media_type="application/json")
    if mode == "stall":
        await asyncio.sleep(STATE["stall_seconds"])

    if streaming:
        return StreamingResponse(_sse(), media_type="text/event-stream", headers=LEAKY_HEADERS)
    return JSONResponse(
        {"id": "chatcmpl-mock", "served_by": key, "choices": [{"message": {"role": "assistant", "content": "hi"}}]},
        headers=LEAKY_HEADERS,
    )
