import pytest
from enrich import get_social_handle, make_session

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


def test_enrichment_success(monkeypatch):
    """Enrichment returns a social_handle when API is OK."""
    session = make_session()
    monkeypatch.setattr(session, "get", lambda *a, **k: DummyResponse(200, {"social_handle": "@alice"}))

    social, reason = get_social_handle(session, "a@b.com")
    assert social == "@alice"
    assert reason == "ok"


def test_enrichment_no_profile(monkeypatch):
    """Enrichment returns no_profile when API gives 404."""
    session = make_session()
    monkeypatch.setattr(session, "get", lambda *a, **k: DummyResponse(404))

    social, reason = get_social_handle(session, "none@b.com")
    assert social is None
    assert reason == "no_profile"


def test_enrichment_unauthorized(monkeypatch):
    """Enrichment returns unauthorized when API gives 401."""
    session = make_session()
    monkeypatch.setattr(session, "get", lambda *a, **k: DummyResponse(401))

    social, reason = get_social_handle(session, "bad@b.com")
    assert social is None
    assert reason == "unauthorized"
