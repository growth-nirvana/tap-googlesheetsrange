# tap-googlesheetsrange

A Singer tap for extracting data from a named range in a Google Sheet using a Google service account.

## Configuration

| Field            | Type   | Required | Description                                                                 |
|------------------|--------|----------|-----------------------------------------------------------------------------|
| spreadsheet_url  | string | Yes      | The public or private URL of the Google Sheet to extract data from.         |
| named_range      | string | Yes      | The named range in the Google Sheet to extract data from.                   |
| credentials      | string | Yes      | Google service account credentials as a JSON string or a file path.         |

### Example `config.json`

```json
{
  "spreadsheet_url": "https://docs.google.com/spreadsheets/d/your-sheet-id/edit#gid=0",
  "named_range": "MyNamedRange",
  "credentials": "/path/to/service_account.json"
}
```

- The `credentials` field can be either a path to a service account JSON file or the JSON string itself.
- The Google Sheet must be shared with the service account email (found in the credentials file under `client_email`).

## Usage

1. Share your Google Sheet with the service account email.
2. Configure the tap as shown above.
3. Run the tap using the Singer SDK or Meltano.

## Output

- The tap emits all rows from the specified named range as Singer records.
- All columns are treated as strings.
- No state/bookmarking is supported; all rows are emitted as-is.

## Local Development

To set up this project for local development:

1. **Clone the repository:**

   ```sh
   git clone https://github.com/your-org/tap-googlesheetsrange.git
   cd tap-googlesheetsrange
   ```

2. **Install dependencies using Poetry (recommended and supported):**

   If you don't have Poetry installed, see: https://python-poetry.org/docs/#installation

   ```sh
   poetry install
   ```

   To activate the virtual environment:
   ```sh
   poetry shell
   ```

   To run the tap in the Poetry environment:
   ```sh
   poetry run tap-googlesheetsrange --config config.json
   ```

3. **Run tests:**

   ```sh
   poetry run pytest
   ```

---

> **Note:** Editable pip installs (`pip install -e .`) are not supported for this project. Use Poetry for all development and testing tasks.

Feel free to open issues or pull requests for improvements! 