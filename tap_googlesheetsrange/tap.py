"""GoogleSheetsRange tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_googlesheetsrange.streams import GoogleSheetsStream

class TapGoogleSheetsNamedRange(Tap):
    """Google Sheets Named Range tap class."""

    name = "tap-googlesheetsrange"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "sheets",
            th.ArrayType(
                th.ObjectType(
                    th.Property("spreadsheet_url", th.StringType, required=True),
                    th.Property("named_range", th.StringType, required=True),
                    th.Property("stream_name", th.StringType, required=False),
                )
            ),
            required=True,
            description="List of sheets and named ranges to extract data from.",
        ),
        th.Property(
            "credentials",
            th.StringType,
            required=True,
            secret=True,
            description="Google service account credentials as a JSON string or a file path.",
        ),
        th.Property(
            "bigquery_column_normalization",
            th.BooleanType,
            required=False,
            default=True,
            description="If true, normalize column names to be BigQuery-compliant.",
        ),
    ).to_dict()

    def discover_streams(self):
        streams = []
        stream_names = []
        name_to_configs = {}
        for sheet_cfg in self.config["sheets"]:
            stream_name = sheet_cfg.get("stream_name") or f"{sheet_cfg['named_range']}"
            stream_names.append(stream_name)
            name_to_configs.setdefault(stream_name, []).append(sheet_cfg)
            streams.append(
                GoogleSheetsStream(
                    self,
                    name=stream_name,
                    sheet_config=sheet_cfg
                )
            )
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
        return streams

if __name__ == "__main__":
    TapGoogleSheetsNamedRange.cli()
