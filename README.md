# tap-googlesheetsrange

`tap-googlesheetsrange` is a Singer tap for GoogleSheetsRange.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPI repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPI:

```bash
pipx install tap-googlesheetsrange
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/tap-googlesheetsrange.git@main
```

-->

## Configuration

### Accepted Config Options

| Field                         | Type    | Required | Description                                                                 |
|-------------------------------|---------|----------|-----------------------------------------------------------------------------|
| sheets                        | array   | Yes      | List of sheet configs. Each config must include `spreadsheet_url` and `named_range`, and may include `stream_name`. **All stream names must be unique.** |
| credentials                   | string  | Yes      | Google service account credentials as a JSON string or a file path.         |
| bigquery_column_normalization | boolean | No       | If true, normalize column names to be BigQuery-compliant. Default: true.    |

Each entry in the `sheets` array should be an object with the following fields:
- `spreadsheet_url` (string, required): The URL of the Google Sheet.
- `named_range` (string, required): The named range to extract.
- `stream_name` (string, optional): The name to use for the stream (defaults to the named range). **Must be unique across all sheets.**

> **Note:**
> All `stream_name` values must be unique across your config. If two or more sheets use the same stream name, the tap will fail to start and provide a detailed error message showing the conflicting entries. This helps prevent data from being overwritten or misrouted.

#### Example `config.json`

```json
{
  "sheets": [
    {
      "spreadsheet_url": "https://docs.google.com/spreadsheets/d/your-sheet-id/edit#gid=0",
      "named_range": "MyNamedRange",
      "stream_name": "optional_stream_name"
    },
    {
      "spreadsheet_url": "https://docs.google.com/spreadsheets/d/another-sheet-id/edit#gid=0",
      "named_range": "AnotherRange"
    }
  ],
  "credentials": "/path/to/service_account.json",
  "bigquery_column_normalization": true
}
```

- The `credentials` field can be either a path to a service account JSON file or the JSON string itself.
- The Google Sheet(s) must be shared with the service account email (found in the credentials file under `client_email`).

### Stream Name Validation and Error Example

If you provide duplicate stream names, the tap will fail to start and show an error like:

```
Duplicate stream names found: duplicate.
Conflicting entries:
  - spreadsheet_url: https://docs.google.com/spreadsheets/d/1, named_range: Range1, stream_name: duplicate
  - spreadsheet_url: https://docs.google.com/spreadsheets/d/2, named_range: Range2, stream_name: duplicate
Stream names must be unique across all sheets in the config.
```

To fix this, ensure each `stream_name` (or `named_range` if `stream_name` is omitted) is unique in your config.

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `tap-googlesheetsrange` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-googlesheetsrange --version
tap-googlesheetsrange --help
tap-googlesheetsrange --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.9+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
uv run pytest
```

You can also test the `tap-googlesheetsrange` CLI interface directly using `uv run`:

```bash
uv run tap-googlesheetsrange --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-googlesheetsrange
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-googlesheetsrange --version

# OR run a test ELT pipeline:
meltano run tap-googlesheetsrange target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.

### Column Normalization

If `bigquery_column_normalization` is set to `true` (the default), all column names will be normalized to be BigQuery-compliant. This includes:
- Lowercasing
- Replacing spaces and special characters with underscores
- Ensuring the name starts with a letter or underscore
- Truncating to 300 characters
- Avoiding forbidden prefixes (e.g., `_TABLE_`, `_FILE_`, etc.) by prepending an extra underscore
- Avoiding reserved keywords (e.g., `WHERE`, `VIEW`) by appending `_col`
- Ensuring uniqueness

This is useful if you plan to load the tap output directly into BigQuery.
