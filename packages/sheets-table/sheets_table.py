"""The table a sheet entry makes, with no third-party dependencies.

Shared by the tap (which loads it) and the Aria runner (which previews it), so a preview shows
exactly the columns and rows a sync loads: header row, blank-row skipping, column names.
"""


from __future__ import annotations

import re
import typing as t
from dataclasses import dataclass, field
from typing import List, Optional, Set
from urllib.parse import parse_qs, urlparse


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
    # The rows at the bottom to leave out: "auto" (find_footer), or how many (0: none).
    footer_rows: int | str = 0

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
            footer_rows=_footer_rows(cfg, str(name)),
        )

    @property
    def skip_footer(self) -> bool:
        return self.footer_rows == "auto" or (isinstance(self.footer_rows, int) and self.footer_rows > 0)

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


def _footer_rows(cfg: dict[str, t.Any], name: str) -> int | str:
    """footer_rows: "auto" or a count; skip_footer (1.5.0's switch) still means "auto"."""
    value = cfg.get("footer_rows")
    if value is None:
        return "auto" if cfg.get("skip_footer") else 0
    if isinstance(value, str) and value.strip().lower() == "auto":
        return "auto"
    if isinstance(value, bool):
        raise ValueError(f"{name}: footer_rows is \"auto\" or how many rows to leave out at the bottom")
    try:
        count = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name}: footer_rows is \"auto\" or how many rows to leave out at the bottom") from None
    if count < 0:
        raise ValueError(f"{name}: footer_rows can't be negative")
    return count


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




@dataclass
class Table:
    """The records one entry makes: column names, rows and counts (no values in logs)."""

    columns: list[str]
    rows: list[dict[str, t.Any]] = field(default_factory=list)
    header_sheet_row: int | None = None
    # footer_rows: the rows left out at the bottom (sheet_row, text), or why none were.
    footer: list[dict[str, t.Any]] = field(default_factory=list)
    footer_kept: str | None = None


# A footer has at most this many rows with text, and never more than the data rows above it.
FOOTER_MAX_ROWS = 10
TOTAL_ROW = re.compile(r"^(grand\s+)?totals?\b", re.IGNORECASE)


def _shape(text: str) -> str:
    """A value's look: runs of digits become 9, of letters a ("006FX01AAA" -> "9a9a")."""
    return re.sub(r"[^\W\d_]+", "a", re.sub(r"\d+", "9", text.strip()))


def find_footer(data: list[list[t.Any]], width: int) -> tuple[int | None, str | None]:
    """Where the footer starts in data (rows under the header), or None, and why none was taken.

    A footer is the block of rows at the very bottom after the last data row, each of them blank,
    a total ("Total 3,020", "Grand Total"), or a line of text in at most two cells that doesn't look
    like that column's values ("Confidential Information - Do Not Distribute", a copyright).
    Only rows at the bottom count: a short row in the middle of the data always loads.
    """
    def filled(row: list[t.Any]) -> list[int]:
        return [i for i, c in enumerate(row[:width]) if not _blank(c)]

    full = [row for row in data if len(filled(row)) > 2]
    usual: dict[int, set[str]] = {}
    for row in full:
        for i in filled(row):
            usual.setdefault(i, set()).add(_shape(_text(row[i])))

    def footer_like(row: list[t.Any]) -> bool:
        cells = filled(row)
        if not cells:
            return True
        if TOTAL_ROW.match(_text(row[cells[0]]).strip()):
            return True
        if width < 3 or len(cells) > 2:
            return False
        return all(_shape(_text(row[i])) not in usual.get(i, set()) for i in cells)

    start = len(data)
    while start > 0 and footer_like(data[start - 1]):
        start -= 1
    footer = data[start:]
    if not footer or all(not filled(row) for row in footer):
        return None, None
    # Blank rows don't count toward the limits; the rows with text do.
    text_rows = sum(1 for row in footer if filled(row))
    remaining = sum(1 for row in data[:start] if filled(row))
    if text_rows > FOOTER_MAX_ROWS:
        return None, f"the last {text_rows} rows look like a footer, more than {FOOTER_MAX_ROWS}: all of them load"
    if text_rows > remaining:
        return None, f"the last {text_rows} rows look like a footer, more than the {remaining} rows above them: all of them load"
    return start, None


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
    if spec.footer_rows == "auto":
        footer_at, footer_kept = find_footer(data, width)
    elif spec.footer_rows and spec.footer_rows >= len(data):
        footer_at, footer_kept = None, f"the table has {len(data)} rows under the header, not more than {spec.footer_rows}: all of them load"
    else:
        footer_at, footer_kept = (len(data) - spec.footer_rows, None) if spec.footer_rows else (None, None)
    footer: list[dict[str, t.Any]] = []
    rows: list[dict[str, t.Any]] = []
    for offset, row in enumerate(data):
        if footer_at is not None and offset >= footer_at:
            text = " ".join(_text(c).strip() for c in row[:width] if not _blank(c))
            footer.append({"sheet_row": (first_data_row + offset) if first_data_row is not None else None, "text": text[:200]})
            continue
        if spec.skip_blank_rows and all(_blank(c) for c in row[:width]):
            continue
        cells = row[:width] + [None] * (width - len(row[:width]))
        record: dict[str, t.Any] = {columns[i]: _text(cells[i]) for i in range(width)}
        record["_row_number"] = offset + 1
        if sheet_rows:
            record["_sheet_row"] = (first_data_row + offset) if first_data_row is not None else None
        rows.append(record)
    header_sheet_row = (read.start_row + header_at) if (read.start_row is not None and header_at is not None) else None
    return Table(columns=columns, rows=rows, header_sheet_row=header_sheet_row, footer=footer, footer_kept=footer_kept)

def normalize_bq_column_names(
    headers: List[str],
    reserved_keywords: Optional[Set[str]] = None,
    forbidden_prefixes: Optional[List[str]] = None,
) -> List[str]:
    """
    Normalize a list of column names to be BigQuery-compliant.

    Args:
        headers: The original column names.
        reserved_keywords: Set of reserved keywords to avoid.
        forbidden_prefixes: List of forbidden prefixes.

    Returns:
        List of normalized column names.
    """
    reserved_keywords = set(reserved_keywords or [])
    forbidden_prefixes = [p.lower() for p in (forbidden_prefixes or [])]
    normalized = []
    seen = {}
    for orig in headers:
        # 1. Lowercase
        col = orig.lower()
        # 2. Replace non-alphanumeric/underscore with _
        col = re.sub(r"[^a-z0-9_]+", "_", col)
        # 3. If starts with forbidden prefix, prepend an extra underscore (before stripping underscores)
        for prefix in forbidden_prefixes:
            if col.startswith(prefix):
                col = f"_{col}"
                break
        # 4. Remove trailing underscores only
        col = col.rstrip("_")
        # 5. If starts with digit, prepend _
        if col and col[0].isdigit():
            col = f"_{col}"
        # 6. If empty after cleaning, use 'column'
        if not col:
            col = "column"
        # 7. If reserved keyword, append _col
        if col in reserved_keywords:
            col = f"{col}_col"
        # 8. Truncate to 300 chars
        col = col[:300]
        # 9. Ensure uniqueness
        base_col = col
        i = 2
        while col in seen:
            col = f"{base_col}_{i}"
            i += 1
        seen[col] = True
        normalized.append(col)
    return normalized
