import pytest
from enrich import push_enriched_row, make_session

class DummyResponse:
    def __init__(self, status_code=200, json_data=None, headers=None, text=""):
        self.status_code = status_code
        self._json = json_data
        self.headers = headers or {}
        self.text = text

    @property
    def ok(self):
        return 200 <= self.status_code < 300

    def json(self):
        if self._json is None:
            raise ValueError("No JSON")
        return self._json


def make_payload():
    return {
        "customer_id": "c1",
        "name": "Alice",
        "email": "a@b.com",
        "total_spend": 100.0,
        "social_handle": "@alice",
    }


def test_push_success(monkeypatch):
    """Push returns pushed_ok when API accepts the payload."""
    session = make_session()
    monkeypatch.setattr(session, "post", lambda *a, **k: DummyResponse(200, {"result": "ok"}))

    success, reason = push_enriched_row(session, make_payload())
    assert success is True
    assert reason == "pushed_ok"


def test_push_unauthorized(monkeypatch):
    """Push returns unauthorized when API key is invalid (401)."""
    session = make_session()
    monkeypatch.setattr(session, "post", lambda *a, **k: DummyResponse(401))

    success, reason = push_enriched_row(session, make_payload())
    assert success is False
    assert reason == "unauthorized"


def test_push_rate_limited(monkeypatch):
    """Push retries on 429 and eventually fails after max attempts."""
    session = make_session()
    monkeypatch.setattr(session, "post", lambda *a, **k: DummyResponse(429, headers={"Retry-After": "1"}))

    success, reason = push_enriched_row(session, make_payload(), max_local_attempts=2)
    assert success is False
    assert "max_attempts" in reason or "429" in reason or "unauthorized" in reason


def test_push_client_error(monkeypatch):
    """Push returns client_error_xxx for 4xx other than 401/429."""
    session = make_session()
    monkeypatch.setattr(session, "post", lambda *a, **k: DummyResponse(400, text="Bad Request"))

    success, reason = push_enriched_row(session, make_payload())
    assert success is False
    assert "client_error" in reason


def test_push_server_error(monkeypatch):
    """Push retries on 5xx and eventually fails after max attempts."""
    session = make_session()
    monkeypatch.setattr(session, "post", lambda *a, **k: DummyResponse(500, text="server error"))

    success, reason = push_enriched_row(session, make_payload(), max_local_attempts=2)
    assert success is False
    assert reason == "push_max_attempts"
