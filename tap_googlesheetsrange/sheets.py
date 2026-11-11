"""Reading one sheet entry: where it points, the values there, and the table they make.

A sheet entry (one stream) points at its cells in one of these ways, first match wins:

- ``named_range``: a named range of the spreadsheet (the tap's original way);
- ``sheet`` (a tab's title), with an optional A1 ``range`` inside it (``A1:F``, ``B3:H200``);
- ``range`` alone: an A1 range, with or without a tab (``Clients!A:F``);
- nothing: the tab the URL points at (``#gid=``), else the first tab.

A whole tab is read as far as it has values (the Sheets API trims empty rows and columns at
the end), so nobody has to create or grow a named range as a sheet gets longer.

The header is the range's first row by default (``header_row: 1``, as before). ``"auto"`` takes
the first row with any value (blank rows above a table are skipped); ``0`` means the range has
no header and columns are named after their letters (``column_a``...).

Reads are grouped per spreadsheet (one ``values:batchGet`` for all of its entries), and each
entry's read is kept for the run, so discovery and sync cost one request per spreadsheet each.
Every failure becomes a ``SheetError`` with a kind a person can act on: ``not_shared``,
``not_found``, ``range_not_found``, ``api_disabled``, ``empty`` or ``error``. Messages never
carry cell values.
"""

from __future__ import annotations

import re
import typing as t
from dataclasses import dataclass, field
from urllib.parse import parse_qs, urlparse

from gspread.exceptions import APIError

from tap_googlesheetsrange.bq_column_normalizer import normalize_bq_column_names

SPREADSHEET_ID = re.compile(r"/spreadsheets/d/([a-zA-Z0-9_-]+)")
BARE_ID = re.compile(r"^[a-zA-Z0-9_-]{20,}$")
# The start cell of the range the API answered with: 'My Tab'!B3:F10, Sheet1!A1:Z1000, A1:C5.
RANGE_START = re.compile(r"^\$?([A-Za-z]{1,3})?\$?(\d+)?")

NOT_SHARED = "not_shared"
NOT_FOUND = "not_found"
RANGE_NOT_FOUND = "range_not_found"
API_DISABLED = "api_disabled"
EMPTY = "empty"
ERROR = "error"
OK = "ok"


class SheetsAuthError(RuntimeError):
    """The credentials themselves were refused (401) or the API is off for their project: no
    sheet can be read, so the run stops instead of failing every stream one by one."""


class SheetError(RuntimeError):
    """One sheet entry can't be read. ``kind`` says why; the message says what to do."""

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


def spreadsheet_id(url_or_id: str) -> str:
    text = (url_or_id or "").strip()
    match = SPREADSHEET_ID.search(text)
    if match:
        return match.group(1)
    if BARE_ID.match(text):
        return text
    raise SheetError(NOT_FOUND, "the spreadsheet URL has no spreadsheet id (https://docs.google.com/spreadsheets/d/<id>/...)")


def url_gid(url: str) -> int | None:
    """The tab a spreadsheet URL points at (#gid=123 or ?gid=123)."""
    parsed = urlparse(url or "")
    for part in (parsed.fragment, parsed.query):
        values = parse_qs(part).get("gid")
        if values and values[0].isdigit():
            return int(values[0])
    return None


def quote_tab(title: str) -> str:
    return "'" + title.replace("'", "''") + "'"


def column_letter(index: int) -> str:
    """0 -> A, 25 -> Z, 26 -> AA."""
    out = ""
    index += 1
    while index:
        index, rem = divmod(index - 1, 26)
        out = chr(65 + rem) + out
    return out


def column_index(letters: str) -> int:
    out = 0
    for ch in letters.upper():
        out = out * 26 + (ord(ch) - 64)
    return out - 1


@dataclass(frozen=True)
class SheetSpec:
    """One configured sheet entry."""

    stream_name: str
    spreadsheet_url: str
    named_range: str | None = None
    sheet: str | None = None
    range: str | None = None
    header_row: int | str = 1
    skip_blank_rows: bool = False
    keep_unnamed_columns: bool = False

    @classmethod
    def from_config(cls, cfg: dict[str, t.Any]) -> SheetSpec:
        url = cfg.get("spreadsheet_url") or cfg.get("spreadsheet_id") or ""
        named = (cfg.get("named_range") or "").strip() or None
        sheet = (cfg.get("sheet") or "").strip() or None
        rng = (cfg.get("range") or "").strip() or None
        name = cfg.get("stream_name") or named or sheet or rng
        if not name:
            raise ValueError(
                f"sheet entry for {url or '(no URL)'} needs a stream_name (or a named_range / sheet to name it after)"
            )
        header = cfg.get("header_row", 1)
        if isinstance(header, str) and header.strip().lower() == "auto":
            header = "auto"
        elif isinstance(header, bool) or not isinstance(header, (int, str)):
            raise ValueError(f"{name}: header_row is a row number (1 is the first), 0 (no header) or \"auto\"")
        else:
            try:
                header = int(header)
            except ValueError:
                raise ValueError(f"{name}: header_row is a row number (1 is the first), 0 (no header) or \"auto\"") from None
            if header < 0:
                raise ValueError(f"{name}: header_row can't be negative")
        return cls(
            stream_name=str(name),
            spreadsheet_url=str(url),
            named_range=named,
            sheet=sheet,
            range=rng,
            header_row=header,
            skip_blank_rows=bool(cfg.get("skip_blank_rows", False)),
            keep_unnamed_columns=bool(cfg.get("keep_unnamed_columns", False)),
        )

    @property
    def spreadsheet_id(self) -> str:
        return spreadsheet_id(self.spreadsheet_url)

    @property
    def needs_tab_lookup(self) -> bool:
        return not (self.named_range or self.sheet or self.range)

    def a1(self, tab_title: str | None = None) -> str:
        """What to ask the API for. tab_title: the URL's (or first) tab, when nothing else is set."""
        if self.named_range:
            return self.named_range
        if self.sheet:
            return quote_tab(self.sheet) + (f"!{self.range}" if self.range else "")
        if self.range:
            return self.range
        if tab_title is None:
            raise SheetError(RANGE_NOT_FOUND, "no tab to read")
        return quote_tab(tab_title)

    def describe(self) -> str:
        if self.named_range:
            return f"named range {self.named_range!r}"
        if self.sheet:
            return f"tab {self.sheet!r}" + (f" range {self.range}" if self.range else "")
        if self.range:
            return f"range {self.range}"
        return "the URL's tab"


@dataclass
class Read:
    """What the API answered for one entry: the values and where they start on the sheet."""

    values: list[list[t.Any]]
    start_row: int | None  # sheet row (1-based) of values[0]; None when unknown
    start_col: int  # 0-based column of values[i][0]
    answered_range: str = ""


def parse_start(answered_range: str) -> tuple[int | None, int]:
    match = RANGE_START.match(answered_range.rsplit("!", 1)[-1])
    if not match or not (match.group(1) or match.group(2)):
        return None, 0
    letters, row = match.group(1), match.group(2)
    return (int(row) if row else (1 if letters else None)), (column_index(letters) if letters else 0)


def classify(err: APIError, spec: SheetSpec, email: str | None) -> SheetError:
    """A person-facing reason for an API error on this entry."""
    status = getattr(getattr(err, "response", None), "status_code", None) or getattr(err, "code", None)
    error = getattr(err, "error", {}) or {}
    message = str(error.get("message") or "")
    reason = str(error.get("status") or "")
    who = email or "the service account"
    if status == 401:
        raise SheetsAuthError(f"Google refused the credentials of {who} (401 UNAUTHENTICATED)")
    if status == 403 and ("has not been used" in message or "disabled" in message or reason == "SERVICE_DISABLED"):
        raise SheetsAuthError(f"the Google Sheets API is not enabled for the project of {who}")
    if status == 403:
        return SheetError(NOT_SHARED, f"the spreadsheet is not shared with {who}: open it, Share, add {who} as a Viewer")
    if status == 404:
        return SheetError(NOT_FOUND, "no spreadsheet at this URL (deleted, or the URL is wrong)")
    if status == 400 and ("Unable to parse range" in message or "range" in message.lower()):
        return SheetError(RANGE_NOT_FOUND, f"the spreadsheet has no {spec.describe()}")
    return SheetError(ERROR, f"the Sheets API answered {status}: {message[:200]}")


@dataclass
class Table:
    """The records one entry makes: column names, rows and counts (no values in logs)."""

    columns: list[str]
    rows: list[dict[str, t.Any]] = field(default_factory=list)
    header_sheet_row: int | None = None


RESERVED_KEYWORDS = {"where", "view"}
FORBIDDEN_PREFIXES = ["_TABLE_", "_FILE_", "_PARTITION", "_ROW_TIMESTAMP", "__ROOT__", "_COLIDENTIFIER"]


def _blank(cell: t.Any) -> bool:
    return cell is None or (isinstance(cell, str) and cell.strip() == "")


def _text(cell: t.Any) -> str:
    if cell is None:
        return ""
    if isinstance(cell, bool):
        return "TRUE" if cell else "FALSE"
    if isinstance(cell, float) and cell.is_integer():
        return str(int(cell))
    return str(cell)


def build_table(
    spec: SheetSpec,
    read: Read,
    *,
    normalize: bool,
    sheet_rows: bool,
) -> Table:
    """The entry's columns and records from what the API answered."""
    values = [list(row) for row in read.values]
    if spec.header_row == "auto":
        header_at = next((i for i, row in enumerate(values) if any(not _blank(c) for c in row)), None)
        if header_at is None:
            raise SheetError(EMPTY, f"the {spec.describe()} has no values")
    elif spec.header_row == 0:
        header_at = None
    else:
        header_at = int(spec.header_row) - 1
        if header_at >= len(values) or not any(not _blank(c) for c in values[header_at]):
            if not values:
                raise SheetError(EMPTY, f"the {spec.describe()} has no values")
            raise SheetError(EMPTY, f"row {spec.header_row} of the {spec.describe()} (the header) is empty")
    data = values[header_at + 1 :] if header_at is not None else values
    raw_header = [_text(c) for c in values[header_at]] if header_at is not None else []
    width = len(raw_header)
    if spec.keep_unnamed_columns or header_at is None:
        width = max([width, *(len(r) for r in data)])
    if width == 0:
        raise SheetError(EMPTY, f"the {spec.describe()} has no values")
    raw_header += [""] * (width - len(raw_header))
    if header_at is None or spec.keep_unnamed_columns:
        # A column with no header text is named after its letter on the sheet.
        raw_header = [
            name if name.strip() else f"column_{column_letter(read.start_col + i).lower()}"
            for i, name in enumerate(raw_header)
        ]
    if normalize:
        columns = normalize_bq_column_names(raw_header, reserved_keywords=RESERVED_KEYWORDS, forbidden_prefixes=FORBIDDEN_PREFIXES)
    else:
        columns = [name if name.strip() else f"column_{i + 1}" for i, name in enumerate(raw_header)]
    reserved = {"_row_number", "_sheet_row"}
    columns = [f"{c}_col" if c in reserved else c for c in columns]

    first_data_row = None
    if read.start_row is not None:
        first_data_row = read.start_row + (header_at + 1 if header_at is not None else 0)
    rows: list[dict[str, t.Any]] = []
    for offset, row in enumerate(data):
        if spec.skip_blank_rows and all(_blank(c) for c in row[:width]):
            continue
        cells = row[:width] + [None] * (width - len(row[:width]))
        record: dict[str, t.Any] = {columns[i]: _text(cells[i]) for i in range(width)}
        record["_row_number"] = offset + 1
        if sheet_rows:
            record["_sheet_row"] = (first_data_row + offset) if first_data_row is not None else None
        rows.append(record)
    header_sheet_row = (read.start_row + header_at) if (read.start_row is not None and header_at is not None) else None
    return Table(columns=columns, rows=rows, header_sheet_row=header_sheet_row)


class SheetsReader:
    """Reads every entry once per run, grouped per spreadsheet; keeps each read or error."""

    def __init__(
        self,
        api: t.Any,
        *,
        value_render_option: str = "FORMATTED_VALUE",
        email: str | None = None,
        call: t.Callable[[t.Callable[..., t.Any]], t.Callable[..., t.Any]] = lambda f: f,
    ) -> None:
        self.api = api
        self.params = {"valueRenderOption": value_render_option, "majorDimension": "ROWS"}
        if value_render_option != "FORMATTED_VALUE":
            self.params["dateTimeRenderOption"] = "FORMATTED_STRING"
        self.email = email

        def guarded(func: t.Callable[..., t.Any]) -> t.Callable[..., t.Any]:
            wrapped = call(func)

            def run(*args: t.Any, **kwargs: t.Any) -> t.Any:
                from google.auth.exceptions import RefreshError

                try:
                    return wrapped(*args, **kwargs)
                except RefreshError:
                    # A bare access token Google refused (expired or not this account's).
                    raise SheetsAuthError(f"Google refused the access token of {email or 'the service account'}") from None

            return run

        self.call = guarded
        self._reads: dict[str, Read | SheetError] = {}
        self._metadata: dict[str, dict[str, t.Any] | SheetError] = {}

    def metadata(self, sid: str, spec: SheetSpec) -> dict[str, t.Any]:
        if sid not in self._metadata:
            try:
                self._metadata[sid] = self.call(self.api.fetch_sheet_metadata)(
                    sid, params={"fields": "properties.title,sheets.properties(sheetId,title,index),namedRanges.name"}
                )
            except APIError as err:
                self._metadata[sid] = classify(err, spec, self.email)
        found = self._metadata[sid]
        if isinstance(found, SheetError):
            raise found
        return found

    def tabs(self, sid: str, spec: SheetSpec) -> list[str]:
        sheets = self.metadata(sid, spec).get("sheets") or []
        return [s.get("properties", {}).get("title", "") for s in sheets]

    def _a1(self, spec: SheetSpec) -> str:
        if not spec.needs_tab_lookup:
            return spec.a1()
        sid = spec.spreadsheet_id
        sheets = self.metadata(sid, spec).get("sheets") or []
        if not sheets:
            raise SheetError(EMPTY, "the spreadsheet has no tabs")
        gid = url_gid(spec.spreadsheet_url)
        chosen = next((s for s in sheets if s.get("properties", {}).get("sheetId") == gid), None) if gid is not None else None
        if gid is not None and chosen is None:
            raise SheetError(RANGE_NOT_FOUND, f"the spreadsheet has no tab with gid {gid} (the URL's tab was deleted?)")
        chosen = chosen or min(sheets, key=lambda s: s.get("properties", {}).get("index", 0))
        return spec.a1(chosen["properties"]["title"])

    def prefetch(self, specs: list[SheetSpec]) -> None:
        """One values:batchGet per spreadsheet for its entries not read yet."""
        groups: dict[str, list[tuple[SheetSpec, str]]] = {}
        for spec in specs:
            if spec.stream_name in self._reads:
                continue
            try:
                groups.setdefault(spec.spreadsheet_id, []).append((spec, self._a1(spec)))
            except SheetError as err:
                self._reads[spec.stream_name] = err
        for sid, entries in groups.items():
            if len(entries) == 1:
                spec, a1 = entries[0]
                self._reads[spec.stream_name] = self._get_one(sid, spec, a1)
                continue
            try:
                answer = self.call(self.api.values_batch_get)(sid, [a1 for _, a1 in entries], params=dict(self.params))
            except APIError as err:
                kind = classify(err, entries[0][0], self.email)
                if kind.kind == RANGE_NOT_FOUND:
                    # One bad range fails the whole batch: read each on its own to say which.
                    for spec, a1 in entries:
                        self._reads[spec.stream_name] = self._get_one(sid, spec, a1)
                    continue
                for spec, _ in entries:
                    self._reads[spec.stream_name] = classify(err, spec, self.email)
                continue
            ranges = answer.get("valueRanges") or []
            for (spec, _), value_range in zip(entries, ranges):
                self._reads[spec.stream_name] = self._to_read(value_range)

    def _get_one(self, sid: str, spec: SheetSpec, a1: str) -> Read | SheetError:
        try:
            return self._to_read(self.call(self.api.values_get)(sid, a1, params=dict(self.params)))
        except APIError as err:
            return classify(err, spec, self.email)

    @staticmethod
    def _to_read(value_range: dict[str, t.Any]) -> Read:
        answered = str(value_range.get("range") or "")
        start_row, start_col = parse_start(answered) if answered else (None, 0)
        return Read(values=value_range.get("values") or [], start_row=start_row, start_col=start_col, answered_range=answered)

    def read(self, spec: SheetSpec) -> Read:
        if spec.stream_name not in self._reads:
            self.prefetch([spec])
        found = self._reads[spec.stream_name]
        if isinstance(found, SheetError):
            if found.kind == RANGE_NOT_FOUND and (spec.sheet or spec.named_range):
                found = self._explain_missing(spec, found)
                self._reads[spec.stream_name] = found
            raise found
        return found

    def _explain_missing(self, spec: SheetSpec, err: SheetError) -> SheetError:
        """Name the tabs (or named ranges) the spreadsheet does have."""
        if "it has" in str(err):
            return err
        try:
            meta = self.metadata(spec.spreadsheet_id, spec)
        except SheetError:
            return err
        if spec.sheet:
            have = [s.get("properties", {}).get("title", "") for s in meta.get("sheets") or []]
            if spec.sheet in have and spec.range:
                return SheetError(RANGE_NOT_FOUND, f"range {spec.range!r} is not a valid A1 range on tab {spec.sheet!r}")
            return SheetError(RANGE_NOT_FOUND, f"the spreadsheet has no tab {spec.sheet!r}; it has {', '.join(map(repr, have)) or 'none'}")
        have = [r.get("name", "") for r in meta.get("namedRanges") or []]
        return SheetError(RANGE_NOT_FOUND, f"the spreadsheet has no named range {spec.named_range!r}; it has {', '.join(map(repr, have)) or 'none'}")
