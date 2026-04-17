"""Stream type classes for tap-googlesheetsrange."""

from __future__ import annotations

import typing as t
import json
import logging
import os
import random
import time
import gspread
from gspread.exceptions import APIError
from singer_sdk import Stream
from singer_sdk import typing as th
from tap_googlesheetsrange.bq_column_normalizer import normalize_bq_column_names

LOGGER = logging.getLogger(__name__)

# Transient HTTP statuses we should retry with backoff. 429 is the one that
# hit us in production (per-minute read quota); 500/502/503/504 are Google
# side hiccups that also resolve on retry.
_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

# Defaults chosen so we cleanly absorb a full 60s/user/min quota window.
_DEFAULT_MAX_RETRIES = 6
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
            except APIError as err:
                status = _status_from_api_error(err)
                if status not in _RETRYABLE_STATUSES or attempt >= max_retries:
                    raise
                server_hint = _retry_after_seconds(err)
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


def _get_shared_client(tap) -> gspread.Client:
    """Return a single gspread client shared across all streams on the tap.

    We used to build a new ``GoogleSheetsAuthenticator`` per stream, which
    performed a separate OAuth token exchange each time. Sharing the client
    keeps auth work to a single request per tap invocation.
    """
    client = getattr(tap, "_gsheets_client", None)
    if client is None:
        client = GoogleSheetsAuthenticator(tap.config["credentials"]).get_client()
        tap._gsheets_client = client
    return client


def _get_shared_spreadsheet(tap, client: gspread.Client, url: str):
    """Return a cached ``Spreadsheet`` for ``url`` to avoid duplicate metadata reads.

    Calling ``client.open_by_url`` triggers a ``fetch_sheet_metadata`` read
    request on the Sheets API. Multiple streams may reference the same
    workbook, so we cache the opened spreadsheet on the tap.
    """
    cache = getattr(tap, "_spreadsheet_cache", None)
    if cache is None:
        cache = {}
        tap._spreadsheet_cache = cache
    if url not in cache:
        cache[url] = with_gsheets_retry(client.open_by_url)(url)
    return cache[url]


def _get_cached_values(tap, spreadsheet, url: str, named_range: str):
    """Return a cached ``values_get`` payload for (url, named_range).

    ``discover_streams`` may run multiple times during a single Meltano-SDK
    invocation (e.g. once for catalog materialization and once more for
    ``sync_all``), which previously issued a fresh ``values_get`` read per
    call. Caching the payload on the tap keeps each named range to a single
    Sheets read per run and is the biggest single reduction in quota usage.
    """
    cache = getattr(tap, "_values_cache", None)
    if cache is None:
        cache = {}
        tap._values_cache = cache
    key = (url, named_range)
    if key not in cache:
        cache[key] = with_gsheets_retry(spreadsheet.values_get)(named_range)
    return cache[key]


class GoogleSheetsStream(Stream):
    name = "named_range_stream"
    primary_keys = []
    replication_key = None

    RESERVED_KEYWORDS = {"where", "view"}
    FORBIDDEN_PREFIXES = ["_TABLE_", "_FILE_", "_PARTITION", "_ROW_TIMESTAMP", "__ROOT__", "_COLIDENTIFIER"]

    def __init__(self, tap, name=None, sheet_config=None):
        self.sheet_config = sheet_config or {}
        self._worksheet = None
        self._header = None
        # Pass a dummy schema to satisfy the SDK; the real schema is provided by the property
        super().__init__(tap, name=(name or self.name), schema={})

    @property
    def authenticator(self):
        """Backwards-compatible accessor that returns a shim with ``get_client``.

        Tests and existing subclasses may call ``self.authenticator.get_client()``.
        We now resolve through the tap-level shared client so every stream uses
        the same underlying gspread session.
        """
        tap = self._tap
        client = _get_shared_client(tap)

        class _SharedClient:
            def get_client(self_inner):
                return client

        return _SharedClient()

    def get_worksheet_and_header(self):
        if self._worksheet is not None and self._header is not None:
            return self._worksheet, self._header
        gc = self.authenticator.get_client()
        url = self.sheet_config["spreadsheet_url"]
        named_range = self.sheet_config["named_range"]
        spreadsheet = _get_shared_spreadsheet(self._tap, gc, url)
        payload = _get_cached_values(self._tap, spreadsheet, url, named_range)
        values = payload.get("values", [])
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
            *[th.Property(col, th.StringType) for col in header],
            th.Property("_row_number", th.IntegerType),
        ).to_dict()

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        values, header = self.get_worksheet_and_header()
        for row_number, row in enumerate(values[1:], start=1):
            # Pad row if shorter than header
            row = row + [None] * (len(header) - len(row))
            record = {header[i]: (row[i] if row[i] is not None else "") for i in range(len(header))}
            record["_row_number"] = row_number
            yield record
