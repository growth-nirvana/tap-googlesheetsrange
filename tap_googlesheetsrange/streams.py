"""Stream type classes for tap-googlesheetsrange."""

from __future__ import annotations

import typing as t
import json
import logging
import os
import random
import time
import gspread
import requests
from gspread.exceptions import APIError
from singer_sdk import Stream
from singer_sdk import typing as th

from tap_googlesheetsrange.sheets import SheetError, SheetSpec, SheetsReader, Table, build_table

LOGGER = logging.getLogger(__name__)

# Transient HTTP statuses we should retry with backoff. 429 is the one that
# hit us in production (per-minute read quota); 500/502/503/504 are Google
# side hiccups that also resolve on retry.
_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

# Defaults chosen so we cleanly absorb a full 60s/user/min quota window.
_DEFAULT_MAX_RETRIES = 8
_DEFAULT_BASE_DELAY = 2.0
_DEFAULT_MAX_DELAY = 64.0


def _status_from_api_error(err: APIError) -> int | None:
    """Extract the HTTP status code from a gspread APIError in a version-safe way."""
    response = getattr(err, "response", None)
    if response is not None and hasattr(response, "status_code"):
        return response.status_code
    code = getattr(err, "code", None)
    if isinstance(code, int):
        return code
    return None


def _retry_after_seconds(err: APIError) -> float | None:
    """Return the Retry-After delay (seconds) from the response, if present."""
    response = getattr(err, "response", None)
    if response is None:
        return None
    headers = getattr(response, "headers", None) or {}
    raw = headers.get("Retry-After") or headers.get("retry-after")
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def with_gsheets_retry(
    func: t.Callable[..., t.Any],
    *,
    max_retries: int = _DEFAULT_MAX_RETRIES,
    base_delay: float = _DEFAULT_BASE_DELAY,
    max_delay: float = _DEFAULT_MAX_DELAY,
    sleep: t.Callable[[float], None] = time.sleep,
) -> t.Callable[..., t.Any]:
    """Wrap a callable so it retries transient Sheets API errors with exponential backoff.

    Honors the ``Retry-After`` header when provided, otherwise uses exponential
    backoff with jitter. Only retries on statuses in ``_RETRYABLE_STATUSES``;
    any other ``APIError`` is re-raised immediately.
    """

    def wrapper(*args: t.Any, **kwargs: t.Any) -> t.Any:
        attempt = 0
        while True:
            try:
                return func(*args, **kwargs)
            except (APIError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as err:
                if isinstance(err, APIError):
                    status = _status_from_api_error(err)
                    retryable = status in _RETRYABLE_STATUSES
                    server_hint = _retry_after_seconds(err)
                else:
                    # A dropped connection or a timeout: the same request again is safe (reads only).
                    status = type(err).__name__
                    retryable = True
                    server_hint = None
                if not retryable or attempt >= max_retries:
                    raise
                if server_hint is not None:
                    delay = min(server_hint, max_delay)
                else:
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    delay += random.uniform(0, delay * 0.25)
                LOGGER.warning(
                    "Sheets API returned %s on %s; retry %d/%d in %.1fs",
                    status,
                    getattr(func, "__name__", "call"),
                    attempt + 1,
                    max_retries,
                    delay,
                )
                sleep(delay)
                attempt += 1

    return wrapper


# Read-only: the tap never writes to a spreadsheet.
READONLY_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


class GoogleSheetsAuthenticator:
    """The service account (credentials: its JSON key, as a string or a file path)."""

    def __init__(self, credentials: str):
        if os.path.isfile(credentials):
            with open(credentials, "r") as f:
                self.creds_dict = json.load(f)
        else:
            self.creds_dict = json.loads(credentials)
        self.gc = gspread.service_account_from_dict(self.creds_dict, scopes=READONLY_SCOPES)

    @property
    def email(self) -> str | None:
        return self.creds_dict.get("client_email")

    def get_client(self):
        return self.gc


def _get_shared_client(tap) -> gspread.Client:
    """One gspread client (one token exchange) for every stream of the run."""
    client = getattr(tap, "_gsheets_client", None)
    if client is None:
        auth = GoogleSheetsAuthenticator(tap.config["credentials"])
        client = auth.get_client()
        tap._gsheets_client = client
        tap._gsheets_email = auth.email
    return client


def _get_reader(tap) -> SheetsReader:
    """The run's reader: each entry is read once (grouped per spreadsheet) and kept."""
    reader = getattr(tap, "_sheets_reader", None)
    if reader is None:
        client = _get_shared_client(tap)
        reader = SheetsReader(
            client.http_client,
            value_render_option=tap.config.get("value_render_option") or "FORMATTED_VALUE",
            email=getattr(tap, "_gsheets_email", None),
            call=with_gsheets_retry,
        )
        tap._sheets_reader = reader
    # Every entry known so far, in one request per spreadsheet (entries read already are kept).
    reader.prefetch(list(getattr(tap, "_sheet_specs", None) or []))
    return reader


class GoogleSheetsStream(Stream):
    """One configured sheet entry. Every column is text; typing happens downstream."""

    name = "named_range_stream"
    primary_keys = []
    replication_key = None

    def __init__(self, tap, name=None, sheet_config=None):
        self.sheet_config = sheet_config or {}
        self.spec = SheetSpec.from_config({**self.sheet_config, "stream_name": name or self.sheet_config.get("stream_name")})
        self._table: Table | None = None
        self.error: SheetError | None = None
        super().__init__(tap, name=(name or self.name), schema={})

    @property
    def sheet_rows(self) -> bool:
        return bool(self.config.get("include_sheet_row", False))

    def table(self) -> Table:
        """The entry's columns and rows; raises SheetError when it can't be read."""
        if self._table is None:
            if self.error is not None:
                raise self.error
            try:
                read = _get_reader(self._tap).read(self.spec)
                self._table = build_table(
                    self.spec,
                    read,
                    normalize=bool(self.config.get("bigquery_column_normalization", True)),
                    sheet_rows=self.sheet_rows,
                )
            except SheetError as err:
                self.error = err
                raise
        return self._table

    def get_worksheet_and_header(self):
        """(rows, column names): kept for callers of the earlier API."""
        table = self.table()
        return table.rows, table.columns

    @property
    def schema(self):
        # Discovery never fails on one entry: an unreadable one has only the row columns, and
        # its sync fails (the tap goes on with the others, then names it).
        try:
            columns = self.table().columns
        except SheetError:
            columns = []
        extra = [th.Property("_sheet_row", th.IntegerType)] if self.sheet_rows else []
        return th.PropertiesList(
            *[th.Property(col, th.StringType) for col in columns],
            th.Property("_row_number", th.IntegerType),
            *extra,
        ).to_dict()

    def status(self) -> dict[str, t.Any]:
        """What a check reports for this entry (counts and names only, never values)."""
        try:
            table = self.table()
        except SheetError as err:
            return {"status": err.kind, "message": str(err), "target": self.spec.describe()}
        return {
            "status": "ok",
            "target": self.spec.describe(),
            "rows": len(table.rows),
            "columns": len(table.columns),
            "column_names": list(table.columns),
            "header_row": table.header_sheet_row,
        }

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        yield from self.table().rows
