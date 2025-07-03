"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import pytest
from tap_googlesheetsrange.streams import GoogleSheetsStream, GoogleSheetsAuthenticator
from singer_sdk.tap_base import Tap

# from singer_sdk.testing import get_tap_test_class

from tap_googlesheetsrange.tap import TapGoogleSheetsNamedRange

SAMPLE_CONFIG = {
    "spreadsheet_url": "dummy_url",
    "named_range": "dummy_range",
    "credentials": "{}"
}


# Run standard built-in tap tests from the SDK:
# TestTapGoogleSheetsNamedRange = get_tap_test_class(
#     tap_class=TapGoogleSheetsNamedRange,
#     config=SAMPLE_CONFIG,
# )


# TODO: Create additional tests as appropriate for your tap.

class DummyTap(Tap):
    name = "dummy"
    config = {
        "spreadsheet_url": "dummy_url",
        "named_range": "dummy_range",
        "credentials": "{}"
    }
    def __init__(self):
        pass

class DummyAuthenticator:
    def __init__(self, *a, **kw):
        pass
    def get_client(self):
        class DummySpreadsheet:
            def values_get(self, named_range):
                # Simulate a header with an empty column between B and D
                return {"values": [["A", "B", "", "D"], ["1", "2", "", "4"]]}
            def open_by_url(self, url):
                return self
        return DummySpreadsheet()

@pytest.fixture
def monkeypatch_auth(monkeypatch):
    monkeypatch.setattr(GoogleSheetsStream, "authenticator", property(lambda self: DummyAuthenticator()))


def test_empty_column_header(monkeypatch_auth):
    config = {
        "spreadsheet_url": "dummy_url",
        "named_range": "dummy_range",
        "credentials": "/dev/null"
    }
    tap = TapGoogleSheetsNamedRange(config=config)
    stream = GoogleSheetsStream(tap)
    _, header = stream.get_worksheet_and_header()
    assert header == ["A", "B", "column_3", "D"]
    records = list(stream.get_records(None))
    assert records == [{"A": "1", "B": "2", "column_3": "", "D": "4"}]
