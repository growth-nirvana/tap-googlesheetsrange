"""An in-memory Sheets API with the gspread HTTPClient methods the tap calls."""

from __future__ import annotations

import re
from types import SimpleNamespace

from gspread.exceptions import APIError

from tap_googlesheetsrange import streams as streams_module


class _Response:
    def __init__(self, status_code: int, message: str, status: str = ""):
        self.status_code = status_code
        self.headers = {}
        self.text = message
        self._body = {"error": {"code": status_code, "message": message, "status": status}}

    def json(self):
        return self._body


def api_error(status_code: int, message: str = "fake", status: str = "") -> APIError:
    return APIError(_Response(status_code, message, status))


class FakeSheets:
    """spreadsheets: id -> {"tabs": [(title, gid)], "named": {name: a1}, "cells": {tab: rows}, "error": APIError}"""

    def __init__(self, spreadsheets: dict):
        self.spreadsheets = spreadsheets
        self.calls: list[tuple] = []

    def _book(self, sid):
        book = self.spreadsheets.get(sid)
        if book is None:
            raise api_error(404, "Requested entity was not found.", "NOT_FOUND")
        if book.get("error"):
            raise book["error"]
        return book

    def fetch_sheet_metadata(self, sid, params=None):
        self.calls.append(("metadata", sid))
        book = self._book(sid)
        return {
            "sheets": [{"properties": {"title": t, "sheetId": g, "index": i}} for i, (t, g) in enumerate(book["tabs"])],
            "namedRanges": [{"name": n} for n in book.get("named", {})],
        }

    def _resolve(self, book, a1):
        if a1 in book.get("named", {}):
            a1 = book["named"][a1]
        if "!" in a1:
            tab, rng = a1.rsplit("!", 1)
        elif a1.startswith("'"):
            tab, rng = a1, ""
        elif a1 in [t for t, _ in book["tabs"]]:
            tab, rng = a1, ""
        else:
            tab, rng = book["tabs"][0][0], a1
        title = tab[1:-1].replace("''", "'") if tab.startswith("'") else tab
        if title not in book["cells"]:
            raise api_error(400, f"Unable to parse range: {a1}", "INVALID_ARGUMENT")
        rows = book["cells"][title]
        start_row, start_col, end_row, end_col = 1, 0, None, None
        if rng:
            m = re.fullmatch(r"([A-Z]?)(\d*)(?::([A-Z]?)(\d*))?", rng)
            if not m:
                raise api_error(400, f"Unable to parse range: {a1}", "INVALID_ARGUMENT")
            start_col = ord(m.group(1)) - 65 if m.group(1) else 0
            start_row = int(m.group(2)) if m.group(2) else 1
            end_col = ord(m.group(3)) - 64 if m.group(3) else None
            end_row = int(m.group(4)) if m.group(4) else None
        values = [r[start_col:end_col] for r in rows[start_row - 1 : end_row]]
        while values and not values[-1]:
            values.pop()
        col = chr(65 + start_col)
        return {"range": f"'{title}'!{col}{start_row}:Z{start_row + max(len(values) - 1, 0)}", "values": values}

    def values_get(self, sid, a1, params=None):
        self.calls.append(("get", sid, a1))
        return self._resolve(self._book(sid), a1)

    def values_batch_get(self, sid, ranges, params=None):
        self.calls.append(("batch", sid, tuple(ranges)))
        book = self._book(sid)
        return {"valueRanges": [self._resolve(book, a1) for a1 in ranges]}


def install(monkeypatch, fake: FakeSheets, email: str = "reader@example.iam.gserviceaccount.com") -> FakeSheets:
    def client(tap):
        tap._gsheets_email = email
        return SimpleNamespace(http_client=fake)

    monkeypatch.setattr(streams_module, "_get_shared_client", client)
    return fake
