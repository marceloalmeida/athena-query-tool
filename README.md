# AWS Athena Query Tool

A Python command-line tool for executing SQL queries against AWS Athena and displaying results in ASCII table format or writing them to CSV/JSON files.

## Features

- Execute SQL queries against AWS Athena
- Display results in ASCII table format
- Export results to CSV or JSON files
- Flexible AWS authentication (environment variables, profiles, IAM roles)
- Automatic retry logic for transient failures
- YAML-based configuration

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
./athena-query config.yaml
```

Or with debug logging:

```bash
./athena-query config.yaml --debug
```

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
