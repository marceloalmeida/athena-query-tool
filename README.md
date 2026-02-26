# AWS Athena Query Tool

A Python tool for executing SQL queries against AWS Athena. Available as both a CLI and a browser-based web UI.

## Features

- Execute SQL queries against AWS Athena
- Display results in ASCII table format (CLI) or interactive table (Web UI)
- Export results to CSV or JSON files
- Flexible AWS authentication (environment variables, profiles, IAM roles)
- Automatic retry logic for transient failures
- Query result caching
- YAML-based configuration

## Installation

```bash
pip install -r requirements.txt
```

## CLI Usage

Run queries defined in your config file:

```bash
./athena-query config.yaml
```

With debug logging:

```bash
./athena-query config.yaml --debug
```

The CLI executes all queries listed in the configuration file and outputs results based on the `output.format` setting (table, csv, or json).

## Web UI Usage

Start the web server:

```bash
python -m athena_query_tool.web_cli
```

With a custom config file:

```bash
python -m athena_query_tool.web_cli --config path/to/config.yaml
```

Then open `http://127.0.0.1:5000` in your browser. The web UI lets you:

- View the active AWS region, database, and workgroup
- Write and execute SQL queries interactively
- Browse results in a table
- Export results as CSV or JSON downloads

## Configuration

Create a YAML configuration file with your Athena settings and queries:

```yaml
aws:
  profile: my-profile  # Optional
  region: us-east-1

athena:
  database: my_database
  workgroup: primary
  output_location: s3://my-bucket/athena-results/

output:
  format: table  # Options: table, csv, json
  file: output.csv  # Optional, only used for csv/json formats

queries:
  - name: sample_query
    sql: SELECT * FROM my_table LIMIT 10
```

See `config.example.yaml` for a complete example.

## Requirements

- Python 3.7+
- AWS credentials configured
- Access to AWS Athena

## Development

Run tests:

```bash
pytest tests/
```

Run property-based tests:

```bash
pytest tests/ -k property
```
