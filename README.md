# tap-sherpaan

tap-sherpaan is a Singer tap for Sherpa.

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this tap is available by running:

```bash
tap-sherpaan --about
```

### Config file example

The config file is a json file and it needs to have `shop_id`, `security_code` and optionally `start_date`:

```json
{
  "shop_id": "your_shop_id",
  "security_code": "your_security_code",
  "start_date": "2000-08-14T21:11:43Z"
}
```

Additional optional configuration options:
- `chunk_size`: Number of records to process in each chunk (default: 200)
- `max_retries`: Maximum number of retry attempts for failed requests (default: 3)
- `retry_wait_min`: Minimum wait time between retries in seconds (default: 4)
- `retry_wait_max`: Maximum wait time between retries in seconds (default: 10)

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's `.env` if the `--config=ENV` is provided, such that config values will be considered if a matching environment variable is set either in the terminal context or in the `.env` file.

## Usage

You can easily run `tap-sherpaan` by itself or in a pipeline.

### Executing the Tap Directly

```bash
tap-sherpaan --version
tap-sherpaan --help
tap-sherpaan --config CONFIG --discover > ./catalog.json
```

## Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

## Create and Run Tests

Create tests within the `tap_sherpaan/tests` subfolder and then run:

```bash
poetry run pytest
```

You can also test the `tap-sherpaan` CLI interface directly using `poetry run`:

```bash
poetry run tap-sherpaan --help
```
