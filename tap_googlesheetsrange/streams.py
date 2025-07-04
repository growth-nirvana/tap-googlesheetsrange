"""Stream type classes for tap-googlesheetsrange."""

from __future__ import annotations

import typing as t
import json
import os
import gspread
from singer_sdk import Stream
from singer_sdk import typing as th
from tap_googlesheetsrange.bq_column_normalizer import normalize_bq_column_names

class GoogleSheetsAuthenticator:
    """Custom authenticator for Google Sheets using a service account."""
    def __init__(self, credentials: str):
        # Credentials can be a JSON string or a file path
        if os.path.isfile(credentials):
            with open(credentials, "r") as f:
                self.creds_dict = json.load(f)
        else:
            self.creds_dict = json.loads(credentials)
        self.gc = gspread.service_account_from_dict(self.creds_dict)

    def get_client(self):
        return self.gc

class GoogleSheetsStream(Stream):
    name = "named_range_stream"
    primary_keys = []
    replication_key = None

    RESERVED_KEYWORDS = {"where", "view"}
    FORBIDDEN_PREFIXES = ["_TABLE_", "_FILE_", "_PARTITION", "_ROW_TIMESTAMP", "__ROOT__", "_COLIDENTIFIER"]

    def __init__(self, tap, name=None):
        self._authenticator = None
        self._worksheet = None
        self._header = None
        # Pass a dummy schema to satisfy the SDK; the real schema is provided by the property
        super().__init__(tap, name=(name or self.name), schema={})

    @property
    def authenticator(self):
        if self._authenticator is None:
            self._authenticator = GoogleSheetsAuthenticator(self.config["credentials"])
        return self._authenticator

    def get_worksheet_and_header(self):
        if self._worksheet is not None and self._header is not None:
            return self._worksheet, self._header
        gc = self.authenticator.get_client()
        spreadsheet = gc.open_by_url(self.config["spreadsheet_url"])
        values = spreadsheet.values_get(self.config["named_range"]).get("values", [])
        if not values:
            raise RuntimeError("No data found in the named range.")
        raw_header = values[0]
        if self.config.get("bigquery_column_normalization", False):
            header = normalize_bq_column_names(
                raw_header,
                reserved_keywords=self.RESERVED_KEYWORDS,
                forbidden_prefixes=self.FORBIDDEN_PREFIXES,
            )
        else:
            header = []
            for idx, col in enumerate(raw_header):
                if col.strip() == "":
                    header.append(f"column_{idx+1}")
                else:
                    header.append(col)
        self._worksheet = values
        self._header = header
        return values, header

    @property
    def schema(self):
        # All columns as string type, dynamically inferred from the header row
        _, header = self.get_worksheet_and_header()
        return th.PropertiesList(
            *[th.Property(col, th.StringType) for col in header]
        ).to_dict()

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        values, header = self.get_worksheet_and_header()
        for row in values[1:]:
            # Pad row if shorter than header
            row = row + [None] * (len(header) - len(row))
            yield {header[i]: (row[i] if row[i] is not None else "") for i in range(len(header))}
