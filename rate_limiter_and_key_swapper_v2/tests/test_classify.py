from keyswap.classify import Kind, classify, parse_retry_after


def k(status, body=b"", headers=None):
    return classify(status, body, headers or {}).kind


def test_ok():
    assert k(200) is Kind.OK


def test_429_burst_vs_quota():
    assert k(429, b'{"error":"Rate limit reached for requests per minute"}') is Kind.RATE_LIMITED
    assert k(429, b'{"error":{"code":"insufficient_quota"}}') is Kind.QUOTA_EXHAUSTED
    # Gemini-style: mentions quota, but it is a per-minute metric -> short cooldown
    assert k(429, b'You exceeded your current quota ... GenerateRequestsPerMinute') is Kind.RATE_LIMITED
    # no body at all -> treat as a burst, never a 12h ban
    assert k(429) is Kind.RATE_LIMITED


def test_auth_and_bad_key():
    assert k(401) is Kind.AUTH_FAILED
    assert k(400, b'{"error":{"message":"API key not valid. Please pass a valid API key."}}') is Kind.AUTH_FAILED
    assert k(403, b'{"error":"permission denied for this resource"}') is Kind.PASSTHROUGH


def test_quota_402_403():
    assert k(402) is Kind.QUOTA_EXHAUSTED
    assert k(403, b"billing account disabled") is Kind.QUOTA_EXHAUSTED


def test_model_not_found():
    assert k(404, b'{"error":{"message":"models/gemini-3.6-flash is not found"}}') is Kind.MODEL_NOT_FOUND
    assert k(400, b"The model `x` does not exist") is Kind.MODEL_NOT_FOUND
    assert k(404, b"no such route") is Kind.PASSTHROUGH  # a 404 that isn't about a model


def test_provider_errors_are_not_key_errors():
    for status in (502, 503, 504, 529):
        assert k(status) is Kind.PROVIDER_ERROR
    assert k(500) is Kind.PASSTHROUGH
    assert k(400, b"bad json") is Kind.PASSTHROUGH


def test_retry_after_parsing():
    assert parse_retry_after({"Retry-After": "30"}) == 30
    assert parse_retry_after({"retry-after-ms": "1500"}) == 1.5
    assert parse_retry_after({"Retry-After": "garbage"}) is None
    assert parse_retry_after({}) is None
    date = "Wed, 21 Oct 2015 07:28:00 GMT"
    assert parse_retry_after({"Retry-After": date}, wall_now=1445412480 - 10) == 10
    assert classify(429, b"", {"Retry-After": "7"}).retry_after == 7
