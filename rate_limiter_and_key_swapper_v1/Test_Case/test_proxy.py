"""
test_proxy.py -- end-to-end tests for rate_limiter_and_key_swapper.py

Spawns a REAL mock provider (mock_upstream.py) and a REAL proxy (uvicorn, one process per
scenario) on random localhost ports, then drives them over HTTP. No real API is contacted.

    pip install fastapi httpx uvicorn pytest
    pytest -v test_proxy.py

Every test that needs a different proxy configuration (short cool-down, tiny timeouts,
no token...) gets its own proxy process via the `make_proxy` factory, because the proxy
reads its settings at import time.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

import httpx
import pytest

ROOT = Path(__file__).resolve().parent
ADMIN_TOKEN = "test-admin-token-0123456789abcdef"
H12, H24 = 12 * 3600, 24 * 3600


def fp(key: str) -> str:
    """Same fingerprint the proxy computes (first 8 hex of sha256)."""
    return hashlib.sha256(key.encode()).hexdigest()[:8]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _clean_env(**overrides: Optional[str]) -> dict:
    """Parent env minus anything that could leak into / change the proxy under test."""
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("API_KEYS", "PROXY_AUTH", "UPSTREAM_", "COOLDOWN_", "REQUEST_",
                                "MAX_", "FORWARD_", "ALLOW_"))}
    env["NO_PROXY"] = env["no_proxy"] = "127.0.0.1,localhost"
    env["PYTHONPATH"] = str(ROOT)
    # Hermetic: a developer's .env next to the script must never re-inject settings the test
    # deliberately removed (e.g. PROXY_AUTH_TOKEN for the "no token configured" scenarios).
    env["DISABLE_DOTENV"] = "1"
    for k, v in overrides.items():
        if v is None:
            env.pop(k, None)
        else:
            env[k] = v
    return env


class Server:
    """A uvicorn subprocess that is killed on .stop()."""

    def __init__(self, target: str, port: int, env: dict) -> None:
        self.port = port
        self.url = f"http://127.0.0.1:{port}"
        # Child output goes to a FILE, never to a pipe nobody reads. An undrained pipe has a small
        # fixed buffer (only a few KB on some platforms); once it fills, the child's next log write
        # blocks its event loop and every in-flight request hangs until the client times out --
        # which looked like an httpx.ReadTimeout under concurrency.
        fd, self.log_path = tempfile.mkstemp(prefix="proxy-test-", suffix=".log")
        os.close(fd)
        self._log = open(self.log_path, "ab")  # append mode: safe for the child to write at any time
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", target, "--host", "127.0.0.1", "--port", str(port),
             "--workers", "1", "--log-level", "warning"],
            cwd=ROOT, env=env, stdout=self._log, stderr=subprocess.STDOUT,
        )

    def output(self) -> str:
        """Everything the server process has printed so far (for diagnostics)."""
        try:
            with open(self.log_path, "rb") as fh:
                return fh.read().decode("utf-8", "replace")
        except OSError:
            return ""

    def wait_ready(self, path: str, timeout: float = 20.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited early:\n{self.output()}")
            try:
                if httpx.get(self.url + path, timeout=1.0, trust_env=False).status_code < 500:
                    return
            except httpx.TransportError:
                time.sleep(0.1)
        raise RuntimeError(f"server did not become ready:\n{self.output()[-2000:]}")

    def stop(self) -> None:
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self._log.close()
        try:
            os.unlink(self.log_path)
        except OSError:
            pass


class Mock:
    """Thin client for the mock provider's control API."""

    def __init__(self, server: Server) -> None:
        self.server = server
        self.http = httpx.Client(base_url=server.url, timeout=10, trust_env=False)

    def reset(self) -> None:
        self.http.post("/_control/reset")

    def set_modes(self, default: Optional[str] = None, stall_seconds: Optional[float] = None, **modes: str) -> None:
        cfg: dict = {"modes": modes}
        if default is not None:
            cfg["default"] = default
        if stall_seconds is not None:
            cfg["stall_seconds"] = stall_seconds
        self.http.post("/_control/config", json=cfg)

    def log(self) -> list:
        return self.http.get("/_control/log").json()

    def served(self) -> list[str]:
        """Keys the upstream saw, in arrival order."""
        return [e["key"] for e in self.log()]


class Proxy:
    def __init__(self, server: Server, token: Optional[str]) -> None:
        self.server, self.token = server, token
        self.client = httpx.Client(base_url=server.url, timeout=30, trust_env=False)

    def _auth(self, token: Optional[str]) -> dict:
        return {"Authorization": f"Bearer {token}"} if token else {}

    def chat(self, stream: bool = False, headers: Optional[dict] = None, body: Optional[dict] = None) -> httpx.Response:
        h = {**self._auth(self.token), **(headers or {})}
        payload = body if body is not None else {"model": "m", "messages": [{"role": "user", "content": "hi"}], "stream": stream}
        return self.client.post("/proxy/v1/chat", json=payload, headers=h)

    def admin_get(self, path: str, token: Optional[str] = ...) -> httpx.Response:  # type: ignore[assignment]
        t = self.token if token is ... else token
        return self.client.get(path, headers=self._auth(t))

    def admin_post(self, path: str, json_body=None, token: Optional[str] = ..., content: Optional[bytes] = None) -> httpx.Response:  # type: ignore[assignment]
        t = self.token if token is ... else token
        if content is not None:
            return self.client.post(path, content=content, headers={**self._auth(t), "Content-Type": "application/json"})
        return self.client.post(path, json=json_body, headers=self._auth(t))

    def benched(self) -> dict:
        return self.admin_get("/proxy/keys/benched").json()


def _diagnostics(p: Proxy, mock: Mock) -> str:
    """State dump for failure messages. Every probe is guarded so a wedged server is reported, not re-raised."""
    parts = []
    for name, probe in (("proxy health", lambda: p.client.get("/proxy/health", timeout=5).text),
                        ("upstream requests seen", lambda: str(len(mock.log())))):
        try:
            parts.append(f"{name}={probe()}")
        except httpx.HTTPError as exc:
            parts.append(f"{name}=<{type(exc).__name__}>")
    parts.append("proxy output (tail):\n" + p.server.output()[-1500:])
    return "; ".join(parts)


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def mock_server():
    srv = Server("mock_upstream:app", free_port(), _clean_env())
    srv.wait_ready("/_control/ping")
    yield srv
    srv.stop()


@pytest.fixture
def mock(mock_server):
    m = Mock(mock_server)
    m.reset()
    return m


@pytest.fixture
def make_proxy(mock_server):
    """Factory: make_proxy(keys=..., token=..., ENV_VAR=...) -> running Proxy."""
    started: list[Server] = []

    def _make(keys: str = "k1,k2,k3", token: Optional[str] = ADMIN_TOKEN, **env: str) -> Proxy:
        overrides: dict = {
            "API_KEYS": keys,
            "UPSTREAM_BASE_URL": mock_server.url,
            "PROXY_AUTH_TOKEN": token,
            "ALLOW_UNAUTHENTICATED": None if token else "1",
        }
        overrides.update(env)
        srv = Server("rate_limiter_and_key_swapper:app", free_port(), _clean_env(**overrides))
        started.append(srv)
        srv.wait_ready("/proxy/health")
        return Proxy(srv, token)

    yield _make
    for s in started:
        s.stop()


# --------------------------------------------------------------------------- #
# 1. Rotation and 429 -> swap
# --------------------------------------------------------------------------- #
def test_round_robin_spreads_load_across_keys(mock, make_proxy):
    p = make_proxy()
    assert all(p.chat().status_code == 200 for _ in range(6))
    assert mock.served() == ["k1", "k2", "k3"] * 2


def test_429_triggers_swap_and_client_never_sees_the_error(mock, make_proxy):
    mock.set_modes(k1="429")
    p = make_proxy()

    r = p.chat()

    assert r.status_code == 200
    assert r.json()["served_by"] == "k2"
    assert mock.served() == ["k1", "k2"]  # tried k1, got 429, instantly retried on k2


def test_quota_403_is_treated_like_429(mock, make_proxy):
    mock.set_modes(k1="quota403")
    p = make_proxy()
    assert p.chat().json()["served_by"] == "k2"
    assert p.benched()["benched_count"] == 1


# --------------------------------------------------------------------------- #
# 2. Cool-down state
# --------------------------------------------------------------------------- #
def test_benched_key_enters_cooldown_with_12_to_24h_window(mock, make_proxy):
    mock.set_modes(k1="429")
    p = make_proxy()
    p.chat()

    b = p.benched()
    assert b["total_keys"] == 3 and b["active_keys"] == 2 and b["benched_count"] == 1
    assert b["benched"][0]["key"] == fp("k1")
    assert H12 - 60 <= b["benched"][0]["available_in_seconds"] <= H24


def test_benched_key_is_not_used_again(mock, make_proxy):
    mock.set_modes(k1="429")
    p = make_proxy()
    for _ in range(9):
        assert p.chat().status_code == 200
    assert mock.served().count("k1") == 1  # only the very first probe


def test_cooldown_expires_and_key_rejoins_rotation(mock, make_proxy):
    # 0.0005h = 1.8s, 0.0006h = 2.16s: a real cool-down, just short enough for a test.
    mock.set_modes(k1="429")
    p = make_proxy(COOLDOWN_MIN_HOURS="0.0005", COOLDOWN_MAX_HOURS="0.0006")
    p.chat()
    assert p.benched()["benched_count"] == 1

    mock.set_modes(k1="ok")  # provider recovered
    time.sleep(2.6)

    before = len(mock.served())
    for _ in range(3):
        assert p.chat().status_code == 200
    assert "k1" in mock.served()[before:]
    assert p.benched()["benched_count"] == 0


def test_concurrent_429s_bench_the_key_exactly_once(mock, make_proxy):
    mock.set_modes(k1="429")
    p = make_proxy()

    # Dedicated client for the burst: 20 threads share it, and a loaded CI box (or antivirus
    # scanning freshly spawned processes) can need far longer than usual to answer 40 requests.
    # Roomy read timeout, and a timeout is reported as data instead of a bare traceback.
    burst = Proxy(p.server, p.token)
    burst.client.close()
    burst.client = httpx.Client(base_url=p.server.url, trust_env=False,
                                timeout=httpx.Timeout(90.0, connect=30.0),
                                limits=httpx.Limits(max_connections=40))

    def call(_: int):
        t0 = time.monotonic()
        try:
            return burst.chat().status_code
        except httpx.TimeoutException as exc:
            return f"{type(exc).__name__} after {time.monotonic() - t0:.1f}s"

    try:
        with ThreadPoolExecutor(20) as ex:
            statuses = list(ex.map(call, range(40)))
    finally:
        burst.client.close()

    assert set(statuses) == {200}, f"statuses={sorted(map(str, set(statuses)))}; {_diagnostics(p, mock)}"
    assert p.benched()["benched_count"] == 1
    before = len(mock.served())
    for _ in range(10):
        p.chat()
    assert "k1" not in mock.served()[before:]


# --------------------------------------------------------------------------- #
# 3. Exhaustion -> 503
# --------------------------------------------------------------------------- #
def test_all_keys_exhausted_returns_503_with_retry_after(mock, make_proxy):
    mock.set_modes(default="429")
    p = make_proxy()

    r = p.chat()

    assert r.status_code == 503
    assert r.json()["error"]["type"] == "all_keys_exhausted"
    assert H12 - 60 <= int(r.headers["retry-after"]) <= H24
    assert len(mock.log()) == 3  # each key probed once, then the proxy gave up

    # Subsequent calls are answered locally: zero further upstream traffic.
    assert p.chat().status_code == 503
    assert len(mock.log()) == 3
    assert p.client.get("/proxy/health").json()["status"] == "degraded"


@pytest.mark.parametrize("mode,status", [("forbidden403", 403), ("400", 400), ("500", 500)])
def test_non_quota_errors_pass_through_without_benching(mock, make_proxy, mode, status):
    mock.set_modes(k1=mode)
    p = make_proxy()

    r = p.chat()

    assert r.status_code == status
    assert p.benched()["benched_count"] == 0
    assert mock.served() == ["k1"]  # no pointless retry on other keys


# --------------------------------------------------------------------------- #
# 4. Admin: list + reinject
# --------------------------------------------------------------------------- #
def test_admin_disabled_when_no_token_configured(mock, make_proxy):
    p = make_proxy(token=None)  # ALLOW_UNAUTHENTICATED=1
    assert p.chat().status_code == 200  # chat open (explicit dev opt-in)
    r = p.admin_get("/proxy/keys/benched", token=None)
    assert r.status_code == 403 and r.json()["error"]["type"] == "admin_disabled"
    assert p.admin_post("/proxy/keys/reinject", {"all": True}, token=None).status_code == 403


def test_admin_requires_valid_token(mock, make_proxy):
    p = make_proxy()
    for token in (None, "wrong-token"):
        r = p.admin_get("/proxy/keys/benched", token=token)
        assert r.status_code == 401 and r.headers["www-authenticate"] == "Bearer"
        assert p.admin_post("/proxy/keys/reinject", {"all": True}, token=token).status_code == 401
    # auth is checked BEFORE the body is parsed: garbage + no auth is 401, not 400
    assert p.admin_post("/proxy/keys/reinject", token=None, content=b"{{not json").status_code == 401


def test_reinject_by_fingerprint_puts_key_back_in_rotation(mock, make_proxy):
    mock.set_modes(k1="429")
    p = make_proxy()
    p.chat()
    assert p.benched()["benched_count"] == 1
    mock.set_modes(k1="ok")

    r = p.admin_post("/proxy/keys/reinject", {"key_id": fp("k1")})
    assert r.status_code == 200 and r.json() == {"status": "reinjected", "key": fp("k1")}
    assert p.benched()["benched_count"] == 0

    before = len(mock.served())
    for _ in range(3):
        p.chat()
    assert "k1" in mock.served()[before:]

    # Second reinject of a now-healthy key is a conflict, not a silent success.
    assert p.admin_post("/proxy/keys/reinject", {"key_id": fp("k1")}).status_code == 409
    # Unknown fingerprint.
    unknown = "00000000" if fp("k1") != "00000000" else "ffffffff"
    assert p.admin_post("/proxy/keys/reinject", {"key_id": unknown}).status_code == 404


def test_reinject_by_raw_key(mock, make_proxy):
    mock.set_modes(k1="429")
    p = make_proxy()
    p.chat()
    r = p.admin_post("/proxy/keys/reinject", {"key": "k1"})
    assert r.status_code == 200 and r.json()["key"] == fp("k1")
    assert p.admin_post("/proxy/keys/reinject", {"key": "nope"}).status_code == 404


def test_reinject_all_clears_the_penalty_box(mock, make_proxy):
    mock.set_modes(k1="429", k2="429")
    p = make_proxy()
    p.chat()  # k1 and k2 benched, served by k3
    assert p.benched()["benched_count"] == 2

    r = p.admin_post("/proxy/keys/reinject", {"all": True})
    assert r.status_code == 200
    assert sorted(r.json()["reinjected"]) == sorted([fp("k1"), fp("k2")])
    assert p.benched()["benched_count"] == 0
    # Idempotent on an empty box.
    assert p.admin_post("/proxy/keys/reinject", {"all": True}).json()["reinjected"] == []


@pytest.mark.parametrize("body", [
    {}, {"key_id": "x", "all": True}, {"key_id": "x", "key": "y"},
    {"all": "yes"}, {"key_id": 123}, {"key": ["k1"]}, ["not", "an", "object"],
])
def test_reinject_rejects_malformed_bodies(mock, make_proxy, body):
    p = make_proxy()
    assert p.admin_post("/proxy/keys/reinject", body).status_code == 400


def test_reinject_rejects_non_json(mock, make_proxy):
    p = make_proxy()
    assert p.admin_post("/proxy/keys/reinject", content=b"{{nope").status_code == 400


def test_health_hides_fingerprints_from_unauthenticated_callers(mock, make_proxy):
    mock.set_modes(k1="429")
    p = make_proxy()
    p.chat()
    public = p.client.get("/proxy/health").json()
    assert public["active_keys"] == 2 and "cooling_down" not in public
    private = p.client.get("/proxy/health", headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}).json()
    assert private["cooling_down"][0]["key"] == fp("k1")


# --------------------------------------------------------------------------- #
# 5. Header safety
# --------------------------------------------------------------------------- #
def test_proxy_token_and_client_headers_never_reach_upstream(mock, make_proxy):
    token = "proxy-secret-token-0123456789"
    p = make_proxy(token=token)

    r = p.chat(headers={
        "Cookie": "sid=cookie-secret",
        "X-Forwarded-For": "203.0.113.9",
        "X-Custom-Secret": "leak-me",
        "Proxy-Authorization": "Basic cHJveHk=",
        "anthropic-version": "2023-06-01",  # explicitly whitelisted: SHOULD pass
    })
    assert r.status_code == 200

    seen = mock.log()[0]["headers"]
    assert seen["authorization"] == "Bearer k1"           # pooled key, not the client's token
    assert seen["anthropic-version"] == "2023-06-01"
    blob = json.dumps(seen)
    for needle in (token, "cookie-secret", "203.0.113.9", "leak-me", "cHJveHk="):
        assert needle not in blob, f"{needle!r} leaked upstream"
    for name in ("cookie", "x-forwarded-for", "x-custom-secret", "proxy-authorization"):
        assert name not in seen


def test_upstream_account_identity_headers_are_stripped_from_responses(mock, make_proxy):
    p = make_proxy()
    r = p.chat()
    assert "set-cookie" not in r.headers
    assert "openai-organization" not in r.headers and "openai-project" not in r.headers
    assert r.headers["x-request-id"] == "req-abc"  # benign headers still flow through


def test_missing_or_wrong_proxy_token_is_rejected_before_any_upstream_call(mock, make_proxy):
    p = make_proxy()
    assert p.client.post("/proxy/v1/chat", json={"model": "m"}).status_code == 401
    assert p.client.post("/proxy/v1/chat", json={"model": "m"}, headers={"Authorization": "Bearer nope"}).status_code == 401
    assert mock.log() == []


# --------------------------------------------------------------------------- #
# 6. Timeouts, limits, failure modes
# --------------------------------------------------------------------------- #
def test_stalled_upstream_gives_504_fast_and_does_not_bench_the_key(mock, make_proxy):
    mock.set_modes(k1="stall", stall_seconds=30)
    p = make_proxy(UPSTREAM_TIMEOUT_SECONDS="1")

    t0 = time.monotonic()
    r = p.chat()

    assert r.status_code == 504 and r.json()["error"]["type"] == "upstream_timeout"
    assert time.monotonic() - t0 < 5
    assert p.benched()["benched_count"] == 0


def test_slow_drip_body_is_cut_off_by_the_total_deadline(mock, make_proxy):
    # Bytes keep arriving every 0.3s, so the per-read timeout (10s) never fires;
    # only the hard REQUEST_DEADLINE_SECONDS can stop this.
    mock.set_modes(k1="drip")
    p = make_proxy(UPSTREAM_TIMEOUT_SECONDS="10", REQUEST_DEADLINE_SECONDS="2")

    t0 = time.monotonic()
    r = p.chat()

    assert r.status_code == 504
    assert time.monotonic() - t0 < 6


def test_unreachable_upstream_gives_502_and_does_not_bench(mock, make_proxy):
    p = make_proxy(UPSTREAM_BASE_URL=f"http://127.0.0.1:{free_port()}")  # nothing listening
    r = p.chat()
    assert r.status_code == 502 and r.json()["error"]["type"] == "upstream_unreachable"
    assert p.benched()["benched_count"] == 0


def test_oversized_request_body_is_rejected_with_413(mock, make_proxy):
    p = make_proxy(MAX_REQUEST_BODY_BYTES="1024")
    r = p.chat(body={"model": "m", "messages": [{"role": "user", "content": "x" * 5000}]})
    assert r.status_code == 413
    assert mock.log() == []


def test_streaming_passes_through_and_still_swaps_keys(mock, make_proxy):
    mock.set_modes(k1="429")
    p = make_proxy()

    with p.client.stream("POST", "/proxy/v1/chat",
                         json={"model": "m", "messages": [], "stream": True},
                         headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}) as r:
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/event-stream")
        assert "set-cookie" not in r.headers
        text = "".join(r.iter_text())

    assert text.count("data:") == 4 and text.rstrip().endswith("data: [DONE]")
    assert mock.served() == ["k1", "k2"]


# --------------------------------------------------------------------------- #
# 7. Startup validation (fail fast on unsafe / broken config)
# --------------------------------------------------------------------------- #
def _import_proxy(tmp_path, **env: Optional[str]) -> subprocess.CompletedProcess:
    full = _clean_env(**env)
    return subprocess.run(
        [sys.executable, "-c", "import rate_limiter_and_key_swapper"],
        cwd=tmp_path, env=full, capture_output=True, text=True, timeout=30,
    )


@pytest.mark.parametrize("env,needle", [
    ({"API_KEYS": ""}, "API_KEYS is empty"),
    ({"API_KEYS": "k1", "PROXY_AUTH_TOKEN": None}, "PROXY_AUTH_TOKEN is not set"),
    ({"API_KEYS": "k1 oops", "PROXY_AUTH_TOKEN": "t" * 20}, "whitespace"),
    ({"API_KEYS": "k1", "PROXY_AUTH_TOKEN": "t" * 20, "UPSTREAM_BASE_URL": "http://api.example.com"}, "cleartext"),
    ({"API_KEYS": "k1", "PROXY_AUTH_TOKEN": "t" * 20, "COOLDOWN_MIN_HOURS": "5", "COOLDOWN_MAX_HOURS": "1"}, "cool-down window"),
    ({"API_KEYS": "k1", "PROXY_AUTH_TOKEN": "t" * 20, "COOLDOWN_MIN_HOURS": "nan"}, "finite"),
    ({"API_KEYS": "k1", "PROXY_AUTH_TOKEN": "t" * 20, "FORWARD_HEADERS": "cookie"}, "credential/hop"),
    ({"API_KEYS": "k1", "PROXY_AUTH_TOKEN": "t" * 20, "UPSTREAM_AUTH_HEADER": "bad header"}, "valid HTTP header"),
])
def test_unsafe_or_invalid_config_refuses_to_start(tmp_path, env, needle):
    res = _import_proxy(tmp_path, **env)
    assert res.returncode != 0
    assert needle in res.stderr


def test_valid_config_imports_cleanly(tmp_path):
    res = _import_proxy(tmp_path, API_KEYS="k1,k2,k1", PROXY_AUTH_TOKEN="t" * 20)
    assert res.returncode == 0, res.stderr
