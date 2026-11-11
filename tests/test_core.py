"""Sheet entries: named ranges, tabs, ranges, headers, blank rows, failures (fake Sheets API)."""

from __future__ import annotations

import json

import pytest

from tap_googlesheetsrange.tap import StreamsFailed, TapGoogleSheetsNamedRange
from tests.fake_sheets import FakeSheets, api_error, install

URL = "https://docs.google.com/spreadsheets/d/book1111111111111111111/edit#gid=0"
URL2 = "https://docs.google.com/spreadsheets/d/book2222222222222222222/edit"


def books():
    return {
        "book1111111111111111111": {
            "tabs": [("Clients", 0), ("Spend 2026", 77)],
            "named": {"dummy_range": "'Clients'!A1:D3", "RangeMulti": "'Clients'!A1:B3"},
            "cells": {
                "Clients": [["A", "B", "", "D"], ["1", "2", "", "4"], ["3", "4", "", "6"]],
                "Spend 2026": [
                    [],
                    ["", "Report"],
                    [],
                    ["Client", "Spend ($)", "Spend ($)", "", "where"],
                    ["Acme", "$1,234.50", "1", "", "x", "extra"],
                    [],
                    ["Beta", "$10", "2"],
                ],
            },
        },
        "book2222222222222222222": {"tabs": [("Sheet1", 0)], "named": {"Range2": "Sheet1!A1:B2"}, "cells": {"Sheet1": [["X", "Y"], ["foo", "bar"]]}},
    }


def tap_for(monkeypatch, sheets, **config):
    fake = install(monkeypatch, FakeSheets(books()))
    tap = TapGoogleSheetsNamedRange(config={"sheets": sheets, "credentials": "/dev/null", **config})
    return tap, fake


def records(tap, name):
    return list(tap.streams[name].get_records(None))


def test_empty_column_header(monkeypatch):
    tap, _ = tap_for(monkeypatch, [{"spreadsheet_url": URL, "named_range": "dummy_range"}])
    stream = tap.discover_streams()[0]
    _, header = stream.get_worksheet_and_header()
    assert header == ["a", "b", "column", "d"]
    assert list(stream.get_records(None))[0] == {"a": "1", "b": "2", "column": "", "d": "4", "_row_number": 1}
    assert "integer" in str(stream.schema["properties"]["_row_number"]).lower()


def test_row_number_increments_and_no_normalization(monkeypatch):
    tap, _ = tap_for(monkeypatch, [{"spreadsheet_url": URL, "named_range": "RangeMulti"}], bigquery_column_normalization=False)
    rows = records(tap, "RangeMulti")
    assert [r["_row_number"] for r in rows] == [1, 2]
    assert rows[0] == {"A": "1", "B": "2", "_row_number": 1}


def test_multiple_sheets_and_stream_names(monkeypatch):
    tap, _ = tap_for(
        monkeypatch,
        [{"spreadsheet_url": URL, "named_range": "RangeMulti", "stream_name": "stream1"}, {"spreadsheet_url": URL2, "named_range": "Range2"}],
        bigquery_column_normalization=False,
    )
    assert set(tap.streams) == {"stream1", "Range2"}
    assert tap.streams["Range2"].get_worksheet_and_header()[1] == ["X", "Y"]


def test_duplicate_stream_names_raise_error(monkeypatch):
    with pytest.raises(ValueError) as excinfo:
        tap_for(
            monkeypatch,
            [
                {"spreadsheet_url": URL, "named_range": "Range1", "stream_name": "duplicate"},
                {"spreadsheet_url": URL2, "named_range": "Range2", "stream_name": "duplicate"},
            ],
        )
    assert "duplicate" in str(excinfo.value).lower() and "stream name" in str(excinfo.value).lower()


def test_tab_with_auto_header_skips_blank_rows_and_keeps_unnamed(monkeypatch):
    tap, _ = tap_for(
        monkeypatch,
        [{"spreadsheet_url": URL, "sheet": "Spend 2026", "stream_name": "spend", "header_row": "auto",
          "skip_blank_rows": True, "keep_unnamed_columns": True}],
        include_sheet_row=True,
    )
    stream = tap.streams["spend"]
    # The first row with a value is row 2 ("Report"); a real table usually starts there.
    assert stream.table().header_sheet_row == 2


def test_tab_with_header_row_and_range(monkeypatch):
    tap, _ = tap_for(
        monkeypatch,
        [{"spreadsheet_url": URL, "sheet": "Spend 2026", "range": "A4:F", "stream_name": "spend", "skip_blank_rows": True,
          "keep_unnamed_columns": True}],
        include_sheet_row=True,
    )
    stream = tap.streams["spend"]
    table = stream.table()
    # Duplicate headers get _2; a blank header is named after its letter; reserved words get _col.
    assert table.columns == ["client", "spend", "spend_2", "column_d", "where_col", "column_f"]
    assert table.header_sheet_row == 4
    rows = records(tap, "spend")
    assert rows == [
        {"client": "Acme", "spend": "$1,234.50", "spend_2": "1", "column_d": "", "where_col": "x", "column_f": "extra",
         "_row_number": 1, "_sheet_row": 5},
        {"client": "Beta", "spend": "$10", "spend_2": "2", "column_d": "", "where_col": "", "column_f": "",
         "_row_number": 3, "_sheet_row": 7},
    ]
    assert stream.schema["properties"]["_sheet_row"]["type"] == ["integer", "null"]


def test_header_row_number_within_tab(monkeypatch):
    tap, _ = tap_for(monkeypatch, [{"spreadsheet_url": URL, "sheet": "Spend 2026", "header_row": 4, "stream_name": "s"}])
    table = tap.streams["s"].table()
    assert table.columns[:3] == ["client", "spend", "spend_2"]
    # Without keep_unnamed_columns, cells past the header are left out (the tap's old behaviour).
    assert "column_f" not in table.columns
    assert len(table.rows) == 3  # blank row kept by default


def test_no_header(monkeypatch):
    tap, _ = tap_for(monkeypatch, [{"spreadsheet_url": URL2, "sheet": "Sheet1", "header_row": 0, "stream_name": "raw"}])
    assert tap.streams["raw"].table().columns == ["column_a", "column_b"]
    assert len(tap.streams["raw"].table().rows) == 2


def test_url_gid_picks_the_tab(monkeypatch):
    url = "https://docs.google.com/spreadsheets/d/book1111111111111111111/edit#gid=77"
    tap, fake = tap_for(monkeypatch, [{"spreadsheet_url": url, "stream_name": "by_gid", "header_row": 4}])
    assert tap.streams["by_gid"].table().columns[0] == "client"
    assert ("metadata", "book1111111111111111111") in fake.calls


def test_one_request_per_spreadsheet(monkeypatch):
    tap, fake = tap_for(
        monkeypatch,
        [
            {"spreadsheet_url": URL, "named_range": "RangeMulti", "stream_name": "a"},
            {"spreadsheet_url": URL, "sheet": "Clients", "stream_name": "b"},
            {"spreadsheet_url": URL2, "sheet": "Sheet1", "stream_name": "c"},
        ],
    )
    for name in ("a", "b", "c"):
        records(tap, name)
        tap.streams[name].schema
    reads = [c for c in fake.calls if c[0] in ("get", "batch")]
    assert reads == [("batch", "book1111111111111111111", ("RangeMulti", "'Clients'")), ("get", "book2222222222222222222", "'Sheet1'")]


def test_statuses_name_what_to_fix(monkeypatch, tmp_path):
    fake = FakeSheets(books())
    fake.spreadsheets["book3333333333333333333"] = {"tabs": [], "cells": {}, "error": api_error(403, "The caller does not have permission", "PERMISSION_DENIED")}
    install(monkeypatch, fake)
    report = tmp_path / "check.json"
    tap = TapGoogleSheetsNamedRange(config={
        "sheets": [
            {"spreadsheet_url": URL, "sheet": "Clients", "stream_name": "ok"},
            {"spreadsheet_url": URL, "sheet": "Clientz", "stream_name": "bad_tab"},
            {"spreadsheet_url": "https://docs.google.com/spreadsheets/d/book3333333333333333333/edit", "sheet": "X", "stream_name": "private"},
            {"spreadsheet_url": "https://docs.google.com/spreadsheets/d/book4444444444444444444/edit", "sheet": "X", "stream_name": "gone"},
            {"spreadsheet_url": URL, "named_range": "Nope", "stream_name": "bad_named"},
        ],
        "credentials": "/dev/null",
        "check_report_path": str(report),
    })
    tap.catalog_dict
    found = json.loads(report.read_text())
    assert found["service_account_email"] == "reader@example.iam.gserviceaccount.com"
    s = found["streams"]
    assert s["ok"] == {"status": "ok", "target": "tab 'Clients'", "rows": 2, "columns": 4, "column_names": ["a", "b", "column", "d"], "header_row": 1}
    assert s["bad_tab"]["status"] == "range_not_found"
    assert "'Clients', 'Spend 2026'" in s["bad_tab"]["message"]
    assert s["private"]["status"] == "not_shared"
    assert "reader@example.iam.gserviceaccount.com" in s["private"]["message"]
    assert s["gone"]["status"] == "not_found"
    assert s["bad_named"]["status"] == "range_not_found" and "'dummy_range'" in s["bad_named"]["message"]
    # Discovery still lists every stream; the unreadable ones have only the row column.
    assert list(tap.streams["private"].schema["properties"]) == ["_row_number"]


def test_sync_goes_on_past_an_unreadable_entry(monkeypatch, capsys):
    tap, _ = tap_for(
        monkeypatch,
        [{"spreadsheet_url": URL, "sheet": "Nope", "stream_name": "bad"}, {"spreadsheet_url": URL2, "sheet": "Sheet1", "stream_name": "good"}],
    )
    with pytest.raises(StreamsFailed):
        tap.sync_all()
    out = capsys.readouterr()
    assert '"stream":"good"' in out.out.replace(" ", "") and '"type":"RECORD"' in out.out.replace(" ", "")
    assert "CRITICAL Streams failed: bad\n" in out.err


def test_empty_tab(monkeypatch):
    fake = FakeSheets(books())
    fake.spreadsheets["book2222222222222222222"]["cells"]["Sheet1"] = []
    install(monkeypatch, fake)
    tap = TapGoogleSheetsNamedRange(config={"sheets": [{"spreadsheet_url": URL2, "sheet": "Sheet1", "stream_name": "e"}], "credentials": "/dev/null"})
    assert tap.streams["e"].status()["status"] == "empty"
