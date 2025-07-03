"""GoogleSheetsRange entry point."""

from __future__ import annotations

from tap_googlesheetsrange.tap import TapGoogleSheetsRange

TapGoogleSheetsRange.cli()
