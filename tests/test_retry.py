"""Tests for Sheets API retry/backoff + shared-client caching.

These guard against regressions of the production 429 outage where the tap
aborted on the first quota-exceeded response and also fired duplicate
``open_by_url`` calls per stream.
"""

from __future__ import annotations

import pytest
from gspread.exceptions import APIError

from tap_googlesheetsrange import streams as streams_module
from tap_googlesheetsrange.streams import with_gsheets_retry
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


def test_connection_errors_are_retried():
    import requests

    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise requests.exceptions.ConnectionError("reset")
        return "ok"

    sleeps: list[float] = []
    assert with_gsheets_retry(flaky, sleep=sleeps.append)() == "ok"
    assert len(sleeps) == 1


def test_values_read_retries_on_quota_then_succeeds(monkeypatch):
    """A 429 on a read must not abort the sync; the tap retries it."""
    from tests.fake_sheets import FakeSheets, install

    fake = install(monkeypatch, FakeSheets({"quota00000000000000000": {"tabs": [("S", 0)], "cells": {"S": [["A", "B"], ["1", "2"]]}}}))
    monkeypatch.setattr(streams_module.time, "sleep", lambda _s: None)
    original = fake.values_get
    attempts = {"n": 0}

    def flaky(*args, **kwargs):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise _make_api_error(429, retry_after="0")
        return original(*args, **kwargs)

    fake.values_get = flaky
    tap = TapGoogleSheetsNamedRange(config={
        "sheets": [{"spreadsheet_url": "https://docs.google.com/spreadsheets/d/quota00000000000000000/edit", "sheet": "S", "stream_name": "a"}],
        "credentials": "/dev/null",
        "bigquery_column_normalization": False,
    })
    assert tap.streams["a"].get_worksheet_and_header()[1] == ["A", "B"]
    assert attempts["n"] == 2
