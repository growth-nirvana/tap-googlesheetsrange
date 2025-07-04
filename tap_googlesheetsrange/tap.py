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
        for sheet_cfg in self.config["sheets"]:
            stream_name = sheet_cfg.get("stream_name") or f"{sheet_cfg['named_range']}"
            streams.append(
                GoogleSheetsStream(
                    self,
                    name=stream_name,
                    sheet_config=sheet_cfg
                )
            )
        return streams

if __name__ == "__main__":
    TapGoogleSheetsNamedRange.cli()
