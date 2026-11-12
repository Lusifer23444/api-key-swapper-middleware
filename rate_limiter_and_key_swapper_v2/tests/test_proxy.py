import json

import httpx
import pytest
from starlette.testclient import TestClient

from fakes import OK, FakeProvider, build
from keyswap.proxy import app_from_env, create_app
from test_stream import SSE, StreamUpstream, make

TOKEN = "t" * 32
AUTH = {"Authorization": f"Bearer {TOKEN}"}


def client_for(fake, keys=("A", "B"), **kw):
    g, _ = build(fake, keys=keys)
    return TestClient(create_app(g, auth_token=TOKEN, **kw)), g


def test_refuses_to_start_without_token_unless_explicitly_allowed():
    g, _ = build(FakeProvider())
    with pytest.raises(ValueError):
        create_app(g)
    create_app(g, allow_unauthenticated=True)  # explicit local-dev opt-in is fine


def test_chat_requires_token_and_checks_scheme_case_insensitively():
    c, _ = client_for(FakeProvider())
    assert c.post("/proxy/v1/chat", json={"model": "m"}).status_code == 401
    assert c.post("/proxy/v1/chat", json={"model": "m"}, headers={"Authorization": "Bearer wrong"}).status_code == 401
    ok = c.post("/proxy/v1/chat", json={"model": "m"}, headers={"Authorization": f"bearer {TOKEN}"})
    assert ok.status_code == 200 and ok.json() == OK


def test_proxy_token_and_cookies_never_reach_upstream():
    fake = FakeProvider()
    c, _ = client_for(fake)
    c.post("/proxy/v1/chat", json={"model": "m"}, headers={**AUTH, "Cookie": "session=1", "X-Forwarded-For": "9.9.9.9"})
    sent = fake.requests[0].headers
    assert TOKEN not in sent["authorization"] and sent["authorization"].startswith("Bearer ")
    assert "cookie" not in sent and "x-forwarded-for" not in sent


def test_429_swaps_key_and_503_is_not_blamed_on_keys():
    c, g = client_for(FakeProvider({"A": [(429, {"error": "rate limit"}, {"Retry-After": "30"})]}))
    assert c.post("/proxy/v1/chat", json={"model": "m"}, headers=AUTH).status_code == 200
    assert {k["key_id"]: k["state"] for k in g.status()} == {"A": "cooling", "B": "active"}

    c2, g2 = client_for(FakeProvider(default=(503, {"error": "down"}, {})))
    r = c2.post("/proxy/v1/chat", json={"model": "m"}, headers=AUTH)
    assert r.status_code == 503 and r.json()["error"]["type"] == "provider_unavailable"
    assert all(k["state"] == "active" for k in g2.status())


def test_all_keys_cooling_gives_503_with_retry_after():
    c, _ = client_for(FakeProvider(default=(429, {"error": "rate limit"}, {"Retry-After": "60"})))
    r = c.post("/proxy/v1/chat", json={"model": "m"}, headers=AUTH)
    assert r.status_code == 503 and r.json()["error"]["type"] == "all_keys_exhausted"
    assert 59 <= int(r.headers["Retry-After"]) <= 61


def test_all_keys_disabled_says_so_distinctly():
    c, _ = client_for(FakeProvider(default=(401, {"error": "bad"}, {})))
    r = c.post("/proxy/v1/chat", json={"model": "m"}, headers=AUTH)
    assert r.status_code == 503 and r.json()["error"]["type"] == "no_usable_keys"


def test_bad_bodies():
    c, _ = client_for(FakeProvider(), max_body_bytes=100)
    assert c.post("/proxy/v1/chat", content=b"not json", headers=AUTH).status_code == 400
    assert c.post("/proxy/v1/chat", json=[1, 2], headers=AUTH).status_code == 400
    big = c.post("/proxy/v1/chat", json={"model": "m", "pad": "x" * 500}, headers=AUTH)
    assert big.status_code == 413


def test_upstream_account_headers_are_stripped_from_the_reply():
    fake = FakeProvider(default=(200, OK, {"openai-organization": "org-secret", "set-cookie": "a=b", "x-ok": "1"}))
    c, _ = client_for(fake)
    r = c.post("/proxy/v1/chat", json={"model": "m"}, headers=AUTH)
    assert "openai-organization" not in r.headers and "set-cookie" not in r.headers and r.headers["x-ok"] == "1"


def test_streaming_through_http_with_key_swap():
    up = StreamUpstream({"A": 429})
    gw = make(up)
    c = TestClient(create_app(gw, auth_token=TOKEN))
    with c.stream("POST", "/proxy/v1/chat", json={"model": "m", "stream": True}, headers=AUTH) as r:
        assert r.status_code == 200
        body = b"".join(r.iter_bytes())
    assert body == b"".join(SSE) and up.keys == ["A", "B"]
    assert all(k["in_flight"] == 0 for k in gw.status())


def test_admin_endpoints_need_a_token_and_fail_closed_without_one():
    c, _ = client_for(FakeProvider())
    assert c.get("/proxy/keys").status_code == 401
    assert c.post("/proxy/keys", json={"key": "x"}).status_code == 401
    g, _ = build(FakeProvider())
    open_app = TestClient(create_app(g, allow_unauthenticated=True))
    assert open_app.get("/proxy/keys").status_code == 403          # no token configured -> admin is OFF
    assert open_app.post("/proxy/v1/chat", json={"model": "m"}).status_code == 200


def test_add_remove_reinstate_keys_at_runtime_over_http():
    c, g = client_for(FakeProvider(), keys=())
    r = c.post("/proxy/keys", json={"key": "brand-new-secret", "key_id": "mine"}, headers=AUTH)
    assert r.status_code == 201 and r.json() == {"key_id": "mine"}
    assert c.post("/proxy/keys", json={"key": "brand-new-secret"}, headers=AUTH).status_code == 409   # duplicate
    assert c.post("/proxy/keys", json={"key": "has space"}, headers=AUTH).status_code == 400
    assert c.post("/proxy/keys", json={"nope": 1}, headers=AUTH).status_code == 400

    assert c.post("/proxy/v1/chat", json={"model": "m"}, headers=AUTH).status_code == 200   # usable immediately
    listing = c.get("/proxy/keys", headers=AUTH).json()["keys"]
    assert [k["key_id"] for k in listing] == ["mine"] and "brand-new-secret" not in json.dumps(listing)

    g.pool.release(g.pool.acquire(), __import__("keyswap").Outcome.AUTH_FAILED)
    assert c.post("/proxy/keys/mine/reinstate", headers=AUTH).status_code == 200
    assert c.post("/proxy/keys/ghost/reinstate", headers=AUTH).status_code == 404
    assert c.delete("/proxy/keys/mine", headers=AUTH).status_code == 200
    assert c.delete("/proxy/keys/mine", headers=AUTH).status_code == 404


def test_health_hides_details_without_admin_token():
    c, _ = client_for(FakeProvider())
    plain = c.get("/proxy/health").json()
    assert plain == {"status": "ok", "total_keys": 2, "active_keys": 2}
    assert "keys" in c.get("/proxy/health", headers=AUTH).json()


# ---- configuration from env / .env ---------------------------------------- #
def test_app_from_env_reads_dotenv_and_real_env_wins(tmp_path):
    dotenv = tmp_path / ".env"
    dotenv.write_text(f"API_KEYS=file-key-1,file-key-2\nPROXY_AUTH_TOKEN={TOKEN}\nKEYSWAP_PROVIDER=gemini\n")
    app = app_from_env({}, str(dotenv))
    assert len(app.state.gateway.status()) == 2
    assert "generativelanguage" in app.state.gateway.provider.base_url

    app2 = app_from_env({"API_KEYS": "real-1"}, str(dotenv))      # real environment overrides the file
    assert len(app2.state.gateway.status()) == 1


def test_app_from_env_works_with_no_dotenv_and_no_keys():
    app = app_from_env({"PROXY_AUTH_TOKEN": TOKEN}, "does-not-exist.env")
    assert app.state.gateway.status() == []                        # keys can be added later over HTTP


def test_disable_dotenv_makes_only_real_env_count(tmp_path):
    dotenv = tmp_path / ".env"
    dotenv.write_text(f"API_KEYS=from-file\nPROXY_AUTH_TOKEN={TOKEN}\n")
    app = app_from_env({"DISABLE_DOTENV": "1", "PROXY_AUTH_TOKEN": TOKEN}, str(dotenv))
    assert app.state.gateway.status() == []


def test_misconfiguration_fails_loudly_at_startup(tmp_path):
    with pytest.raises(ValueError):
        app_from_env({"API_KEYS": "k"}, "none.env")                # no token, not explicitly allowed
    with pytest.raises(RuntimeError):
        app_from_env({"PROXY_AUTH_TOKEN": TOKEN, "KEYSWAP_PROVIDER": "nope"}, "none.env")
    with pytest.raises(RuntimeError):
        app_from_env({"PROXY_AUTH_TOKEN": TOKEN, "KEYSWAP_PROVIDER": "custom",
                      "UPSTREAM_BASE_URL": "http://example.com"}, "none.env")   # plain http to a remote host
    with pytest.raises(RuntimeError):
        app_from_env({"PROXY_AUTH_TOKEN": TOKEN, "MODEL_ALIASES": "{bad json"}, "none.env")
    with pytest.raises(RuntimeError):
        app_from_env({"PROXY_AUTH_TOKEN": TOKEN, "UPSTREAM_TIMEOUT_SECONDS": "abc"}, "none.env")


def test_model_aliases_from_env_drive_fallback():
    app = app_from_env({"PROXY_AUTH_TOKEN": TOKEN, "API_KEYS": "k1",
                        "MODEL_ALIASES": '{"fast": ["new-model", "old-model"]}'}, "none.env")
    assert app.state.gateway.models.aliases() == {"fast": ["new-model", "old-model"]}
