# dbt-bq-sourcegen

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

Create or update dbt source YAML from BigQuery.

## Overview

`dbt-bq-sourcegen` automatically generates dbt source YAML files from BigQuery schemas, preserving existing configurations during updates.
It supports table filtering with wildcard patterns and maintains YAML formatting/comments.
This tool streamlines the process of keeping dbt source definitions in sync with your BigQuery datasets.

## Installation

```bash
pip install dbt-bq-sourcegen
```

## Usage

### Apply (create or update automatically)

```bash
dbt-bq-sourcegen apply \
  --project-id your-project \
  --schema your_dataset \
  --output models/staging/your_dataset/src_your_dataset.yml
```

### With table filtering

```bash
# Only include tables matching pattern
dbt-bq-sourcegen apply \
  --project-id your-project \
  --schema your_dataset \
  --table-pattern "stg_*" \
  --output models/staging/your_dataset/src_your_dataset.yml

# Exclude specific tables
dbt-bq-sourcegen apply \
  --project-id your-project \
  --schema your_dataset \
  --exclude "temp" \
  --output models/staging/your_dataset/src_your_dataset.yml
```

## Options

- `--project-id`: Google Cloud project ID (required)
- `--schema`, `--dataset`: BigQuery schema/dataset name (required)
- `--output`: Output YAML file path (required)
- `--table-pattern`: Table name pattern (e.g., 'stg_*')
- `--exclude`: Exclude tables containing this string

## Features

- Automatically generates dbt source YAML from BigQuery schema
- Updates existing source YAML files while preserving custom configurations
- Supports table filtering with wildcard patterns
- Preserves YAML formatting and comments
- Pure Python implementation with clean separation of concerns

## Development

```bash
# Install development dependencies
uv sync

# Run tests
uv run pytest

# Format code
uv run ruff format src/
```
