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
            "spreadsheet_url",
            th.StringType,
            required=True,
            description="The public or private URL of the Google Sheet to extract data from.",
        ),
        th.Property(
            "named_range",
            th.StringType,
            required=True,
            description="The named range in the Google Sheet to extract data from.",
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
            default=False,
            description="If true, normalize column names to be BigQuery-compliant.",
        ),
    ).to_dict()

    def discover_streams(self):
        """Return a list of discovered streams."""
        return [
            GoogleSheetsStream(self, name="named_range_stream"),
        ]

if __name__ == "__main__":
    TapGoogleSheetsNamedRange.cli()
