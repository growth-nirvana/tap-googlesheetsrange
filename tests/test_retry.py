"""Tests for Sheets API retry/backoff + shared-client caching.

These guard against regressions of the production 429 outage where the tap
aborted on the first quota-exceeded response and also fired duplicate
``open_by_url`` calls per stream.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from gspread.exceptions import APIError

from tap_googlesheetsrange import streams as streams_module
from tap_googlesheetsrange.streams import (
    GoogleSheetsStream,
    with_gsheets_retry,
)
from tap_googlesheetsrange.tap import TapGoogleSheetsNamedRange


class _FakeResponse:
    def __init__(self, status_code: int, retry_after: str | None = None):
        self.status_code = status_code
        self.headers = {}
        if retry_after is not None:
            self.headers["Retry-After"] = retry_after

    # gspread's APIError constructor calls response.json(); provide a stub.
    def json(self):
        return {"error": {"code": self.status_code, "message": "fake"}}


def _make_api_error(status: int, retry_after: str | None = None) -> APIError:
    return APIError(_FakeResponse(status, retry_after))


def test_retry_succeeds_after_transient_429():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise _make_api_error(429)
        return "ok"

    sleeps: list[float] = []
    wrapped = with_gsheets_retry(flaky, sleep=sleeps.append)

    assert wrapped() == "ok"
    assert calls["n"] == 3
    assert len(sleeps) == 2
    assert all(s > 0 for s in sleeps)


def test_retry_honors_retry_after_header():
    def always_429():
        raise _make_api_error(429, retry_after="7")

    sleeps: list[float] = []
    wrapped = with_gsheets_retry(
        always_429, max_retries=1, sleep=sleeps.append
    )
    with pytest.raises(APIError):
        wrapped()
    assert sleeps == [7.0]


def test_retry_gives_up_after_max_retries():
    calls = {"n": 0}

    def always_fail():
        calls["n"] += 1
        raise _make_api_error(429)

    sleeps: list[float] = []
    wrapped = with_gsheets_retry(
        always_fail, max_retries=2, sleep=sleeps.append
    )
    with pytest.raises(APIError):
        wrapped()
    assert calls["n"] == 3  # initial + 2 retries


def test_non_retryable_error_not_retried():
    calls = {"n": 0}

    def permission_denied():
        calls["n"] += 1
        raise _make_api_error(403)

    sleeps: list[float] = []
    wrapped = with_gsheets_retry(permission_denied, sleep=sleeps.append)
    with pytest.raises(APIError):
        wrapped()
    assert calls["n"] == 1
    assert sleeps == []


class _CountingSpreadsheet:
    """Spreadsheet stub that tracks how many times values_get was called."""

    def __init__(self):
        self.values_calls: list[str] = []

    def values_get(self, named_range: str):
        self.values_calls.append(named_range)
        return {"values": [["A", "B"], ["1", "2"]]}


class _CountingClient:
    """gspread client stub that tracks open_by_url invocations per URL."""

    def __init__(self):
        self.open_calls: list[str] = []
        self._sheets: dict[str, _CountingSpreadsheet] = {}

    def open_by_url(self, url: str):
        self.open_calls.append(url)
        if url not in self._sheets:
            self._sheets[url] = _CountingSpreadsheet()
        return self._sheets[url]


def _install_fake_client(monkeypatch) -> _CountingClient:
    """Force the tap to use a counting in-memory client instead of real gspread."""
    client = _CountingClient()
    monkeypatch.setattr(
        streams_module,
        "_get_shared_client",
        lambda tap: client,
    )
    # Route the legacy authenticator property to the same client for any
    # code path that still goes through it.
    monkeypatch.setattr(
        GoogleSheetsStream,
        "authenticator",
        property(lambda self: SimpleNamespace(get_client=lambda: client)),
    )
    return client


def test_open_by_url_deduplicated_across_streams(monkeypatch):
    """Two streams on the same spreadsheet should only call open_by_url once."""
    client = _install_fake_client(monkeypatch)
    shared_url = "https://docs.google.com/spreadsheets/d/shared"
    config = {
        "sheets": [
            {
                "spreadsheet_url": shared_url,
                "named_range": "RangeA",
                "stream_name": "stream_a",
            },
            {
                "spreadsheet_url": shared_url,
                "named_range": "RangeB",
                "stream_name": "stream_b",
            },
            {
                "spreadsheet_url": "https://docs.google.com/spreadsheets/d/other",
                "named_range": "RangeC",
                "stream_name": "stream_c",
            },
        ],
        "credentials": "/dev/null",
        "bigquery_column_normalization": False,
    }
    tap = TapGoogleSheetsNamedRange(config=config)
    for stream in tap.discover_streams():
        stream.get_worksheet_and_header()

    # 3 streams but only 2 distinct URLs -> at most 2 open_by_url reads.
    assert client.open_calls.count(shared_url) == 1
    assert len(client.open_calls) == 2


def test_values_get_retries_on_quota_then_succeeds(monkeypatch):
    """A 429 on values_get must not abort the sync; the tap should retry."""
    client = _install_fake_client(monkeypatch)
    monkeypatch.setattr(streams_module.time, "sleep", lambda _s: None)

    url = "https://docs.google.com/spreadsheets/d/quota"
    sheet = _CountingSpreadsheet()

    attempts = {"n": 0}
    original_values_get = sheet.values_get

    def flaky_values_get(named_range: str):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise _make_api_error(429, retry_after="0")
        return original_values_get(named_range)

    sheet.values_get = flaky_values_get  # type: ignore[assignment]
    client._sheets[url] = sheet

    config = {
        "sheets": [
            {
                "spreadsheet_url": url,
                "named_range": "RangeA",
                "stream_name": "stream_a",
            },
        ],
        "credentials": "/dev/null",
        "bigquery_column_normalization": False,
    }
    tap = TapGoogleSheetsNamedRange(config=config)
    stream = tap.discover_streams()[0]
    _, header = stream.get_worksheet_and_header()
    assert header == ["A", "B"]
    assert attempts["n"] == 2
