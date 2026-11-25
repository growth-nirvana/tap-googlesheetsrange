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

import typing as t

from gspread.exceptions import APIError

# The table logic is shared with the Aria runner's preview (packages/sheets-table).
from sheets_table import (  # noqa: F401
    SPREADSHEET_ID,
    BARE_ID,
    RANGE_START,
    NOT_SHARED,
    NOT_FOUND,
    RANGE_NOT_FOUND,
    API_DISABLED,
    EMPTY,
    ERROR,
    OK,
    SheetError,
    spreadsheet_id,
    url_gid,
    quote_tab,
    column_letter,
    column_index,
    SheetSpec,
    Read,
    parse_start,
    Table,
    RESERVED_KEYWORDS,
    FORBIDDEN_PREFIXES,
    build_table,
    _blank,
    _text,
)

class SheetsAuthError(RuntimeError):
    """The credentials themselves were refused (401) or the API is off for their project: no
    sheet can be read, so the run stops instead of failing every stream one by one."""

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
