"""GoogleSheetsRange tap class."""

from __future__ import annotations

import json
import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_googlesheetsrange.sheets import SheetError, SheetSpec
from tap_googlesheetsrange.streams import GoogleSheetsStream, _get_reader


class StreamsFailed(SystemExit):
    """Some entries couldn't be read; the others synced."""


class TapGoogleSheetsNamedRange(Tap):
    """Google Sheets tap: one stream per configured sheet entry."""

    name = "tap-googlesheetsrange"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "sheets",
            th.ArrayType(
                th.ObjectType(
                    th.Property("spreadsheet_url", th.StringType, required=False,
                                description="The spreadsheet's URL (or its id). A #gid= in it picks the tab when no sheet, named_range or range is set."),
                    th.Property("named_range", th.StringType, required=False, description="A named range of the spreadsheet."),
                    th.Property("sheet", th.StringType, required=False, description="A tab's title. The whole tab is read unless range is set."),
                    th.Property("range", th.StringType, required=False, description="An A1 range, in sheet's tab (A1:F, B3:H200) or with its own (Tab!A:F)."),
                    th.Property("stream_name", th.StringType, required=False),
                    th.Property(
                        "header_row",
                        th.CustomType({"type": ["integer", "string"]}),
                        required=False,
                        description='The header\'s row within the range: 1 (default) is its first row, 0 is no header, "auto" the first row with a value.',
                    ),
                    th.Property("skip_blank_rows", th.BooleanType, required=False, description="Leave out rows with no value (default false)."),
                    th.Property(
                        "keep_unnamed_columns",
                        th.BooleanType,
                        required=False,
                        description="Keep values in columns with no header text, named after their letter (column_g). Default false: dropped.",
                    ),
                )
            ),
            required=True,
            description="The sheet entries; each is one stream.",
        ),
        th.Property(
            "credentials",
            th.StringType,
            required=False,
            secret=True,
            description="Google service account credentials as a JSON string or a file path. Or access_token.",
        ),
        th.Property(
            "access_token",
            th.StringType,
            required=False,
            secret=True,
            description="An OAuth access token for the service account (minted by impersonation, so no key is kept). Instead of credentials.",
        ),
        th.Property(
            "service_account_email",
            th.StringType,
            required=False,
            description="With access_token: the account's email, for messages (\"share it with ...\").",
        ),
        th.Property(
            "bigquery_column_normalization",
            th.BooleanType,
            required=False,
            default=True,
            description="If true, normalize column names to be BigQuery-compliant.",
        ),
        th.Property(
            "value_render_option",
            th.StringType,
            required=False,
            allowed_values=["FORMATTED_VALUE", "UNFORMATTED_VALUE"],
            description="FORMATTED_VALUE (default): values as the sheet shows them ($1,234.50, 45%). UNFORMATTED_VALUE: numbers as numbers (1234.5, 0.45); dates stay formatted.",
        ),
        th.Property(
            "include_sheet_row",
            th.BooleanType,
            required=False,
            default=False,
            description="Add _sheet_row, each record's row number on the sheet.",
        ),
        th.Property(
            "check_report_path",
            th.StringType,
            required=False,
            description="Discovery writes each entry's status here (JSON: stream -> status, message, rows, columns). No values.",
        ),
    ).to_dict()

    def discover_streams(self):
        streams = []
        name_to_configs = {}
        # Every entry, before any stream reads its own: the first read fetches them all, one
        # request per spreadsheet.
        self._sheet_specs = []
        for sheet_cfg in self.config["sheets"]:
            name = sheet_cfg.get("stream_name") or sheet_cfg.get("named_range") or sheet_cfg.get("sheet") or sheet_cfg.get("range")
            if name:
                self._sheet_specs.append(SheetSpec.from_config({**sheet_cfg, "stream_name": name}))
        for sheet_cfg in self.config["sheets"]:
            stream_name = sheet_cfg.get("stream_name") or sheet_cfg.get("named_range") or sheet_cfg.get("sheet") or sheet_cfg.get("range")
            if not stream_name:
                raise ValueError(
                    f"sheet entry for {sheet_cfg.get('spreadsheet_url') or '(no URL)'} needs a stream_name, named_range or sheet"
                )
            name_to_configs.setdefault(stream_name, []).append(sheet_cfg)
            streams.append(GoogleSheetsStream(self, name=stream_name, sheet_config=sheet_cfg))
        # Check for duplicate stream names
        duplicates = [name for name, cfgs in name_to_configs.items() if len(cfgs) > 1]
        if duplicates:
            msg_lines = [f"Duplicate stream names found: {', '.join(duplicates)}."]
            msg_lines.append("Conflicting entries:")
            for name in duplicates:
                for cfg in name_to_configs[name]:
                    msg_lines.append(
                        f"  - spreadsheet_url: {cfg.get('spreadsheet_url')}, named_range: {cfg.get('named_range')}, stream_name: {cfg.get('stream_name') or cfg.get('named_range')}"
                    )
            msg_lines.append("Stream names must be unique across all sheets in the config.")
            raise ValueError("\n".join(msg_lines))
        self._sheet_streams = streams
        return streams

    def statuses(self) -> dict[str, dict]:
        """Each entry's status (reads every entry, grouped per spreadsheet)."""
        streams = getattr(self, "_sheet_streams", None) or list(self.streams.values())
        out = {}
        for stream in streams:
            out[stream.name] = stream.status()
            found = out[stream.name]
            if found["status"] == "ok":
                self.logger.info("%s: %s, %s rows, %s columns", stream.name, found["target"], found["rows"], found["columns"])
            else:
                self.logger.warning("%s: %s: %s", stream.name, found["status"], found["message"])
        return out

    def write_check_report(self) -> None:
        path = self.config.get("check_report_path")
        if not path:
            return
        with open(path, "w", encoding="utf-8") as handle:
            json.dump({"service_account_email": getattr(self, "_gsheets_email", None), "streams": self.statuses()}, handle)

    @property
    def catalog_dict(self) -> dict:
        catalog = super().catalog_dict
        self.write_check_report()
        return catalog

    def sync_all(self) -> None:
        """Sync every selected stream; an entry that can't be read doesn't stop the others.

        Afterwards the tap names the entries it gave up on (``CRITICAL Streams failed: a, b``, the
        line runners read) and exits 1, so nothing downstream takes their missing rows as empty.
        """
        failed: list[tuple[str, SheetError]] = []
        streams = self.streams
        original = {name: stream.sync for name, stream in streams.items()}

        def guarded(name):
            def sync(*args, **kwargs):
                try:
                    return original[name](*args, **kwargs)
                except SheetError as err:
                    failed.append((name, err))
                    self.logger.error("%s: %s: %s", name, err.kind, err)
                    return None

            return sync

        for name, stream in streams.items():
            stream.sync = guarded(name)
        try:
            super().sync_all()
        finally:
            for name, stream in streams.items():
                stream.sync = original[name]
        if failed:
            sys.stderr.write("CRITICAL Streams failed: " + ", ".join(name for name, _ in failed) + "\n")
            sys.stderr.flush()
            raise StreamsFailed(1)


if __name__ == "__main__":
    TapGoogleSheetsNamedRange.cli()
