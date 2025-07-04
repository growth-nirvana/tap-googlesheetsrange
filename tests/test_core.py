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
    def get_client(self):
        class DummySpreadsheet:
            def values_get(self, named_range):
                if named_range == "dummy_range":
                    return {"values": [["A", "B", "", "D"], ["1", "2", "", "4"]]}
                if named_range == "Range1":
                    return {"values": [["A", "B"], ["1", "2"]]}
                elif named_range == "Range2":
                    return {"values": [["X", "Y"], ["foo", "bar"]]}
                return {"values": [[]]}
            def open_by_url(self, url):
                return self
        return DummySpreadsheet()

@pytest.fixture
def monkeypatch_auth(monkeypatch):
    monkeypatch.setattr(GoogleSheetsStream, "authenticator", property(lambda self: DummyAuthenticator()))


def test_empty_column_header(monkeypatch_auth):
    config = {
        "sheets": [
            {
                "spreadsheet_url": "dummy_url",
                "named_range": "dummy_range"
            }
        ],
        "credentials": "/dev/null"
    }
    tap = TapGoogleSheetsNamedRange(config=config)
    stream = tap.discover_streams()[0]
    _, header = stream.get_worksheet_and_header()
    assert header == ["a", "b", "column", "d"]
    records = list(stream.get_records(None))
    assert records == [{"a": "1", "b": "2", "column": "", "d": "4"}]

def test_multiple_sheets(monkeypatch_auth):
    config = {
        "sheets": [
            {
                "spreadsheet_url": "url1",
                "named_range": "Range1",
                "stream_name": "stream1"
            },
            {
                "spreadsheet_url": "url2",
                "named_range": "Range2"
            }
        ],
        "credentials": "/dev/null",
        "bigquery_column_normalization": False
    }
    tap = TapGoogleSheetsNamedRange(config=config)
    streams = tap.discover_streams()
    assert len(streams) == 2
    assert streams[0].name == "stream1"
    assert streams[1].name == "Range2"
    _, header1 = streams[0].get_worksheet_and_header()
    _, header2 = streams[1].get_worksheet_and_header()
    assert header1 == ["A", "B"]
    assert header2 == ["X", "Y"]

def test_duplicate_stream_names_raise_error(monkeypatch_auth):
    config = {
        "sheets": [
            {
                "spreadsheet_url": "https://docs.google.com/spreadsheets/d/1",
                "named_range": "Range1",
                "stream_name": "duplicate"
            },
            {
                "spreadsheet_url": "https://docs.google.com/spreadsheets/d/2",
                "named_range": "Range2",
                "stream_name": "duplicate"
            }
        ],
        "credentials": "/dev/null",  # Use /dev/null for consistency with other tests
        "normalize_columns": True
    }
    with pytest.raises(ValueError) as excinfo:
        tap = TapGoogleSheetsNamedRange(config=config)
    assert "duplicate" in str(excinfo.value).lower() and "stream name" in str(excinfo.value).lower()
