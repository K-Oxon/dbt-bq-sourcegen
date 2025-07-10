# dbt-bq-sourcegen

Create or update BigQuery source YAML for dbt.

## Installation

```bash
pip install git+https://github.com/K-Oxon/dbt-bq-sourcegen.git
```

## Usage

### Create a new source YAML file

```bash
dbt-bq-sourcegen create \
  --project-id your-project \
  --dataset your_dataset \
  --output models/staging/your_dataset/src_your_dataset.yml
```

### Update an existing source YAML file

```bash
dbt-bq-sourcegen update \
  --project-id your-project \
  --source-yml models/staging/your_dataset/src_your_dataset.yml \
  --sync-columns  # Sync column information
  --remove-deleted  # Remove tables/columns not in BigQuery
```

### Apply (create or update automatically)

```bash
dbt-bq-sourcegen apply \
  --project-id your-project \
  --dataset your_dataset \
  --output models/staging/your_dataset/src_your_dataset.yml
```

## Options

- `--project-id`: Google Cloud project ID (required)
- `--dataset`: BigQuery dataset name (required for create/apply)
- `--output`: Output YAML file path (required for create/apply)
- `--source-yml`: Existing source YAML file (required for update)
- `--table-pattern`: Table name pattern (e.g., 'stg_*')
- `--exclude`: Exclude tables containing this string
- `--sync-columns`: Sync column information
- `--remove-deleted`: Remove tables/columns not in BigQuery

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
