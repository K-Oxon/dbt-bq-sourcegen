"""
CLI interface for dbt-bq-sourcegen
"""

import logging
from pathlib import Path

import click

from .io.bigquery import BigQueryClient
from .io.yaml_handler import YamlHandler
from .logic.merge_strategy import merge_source_file
from .logic.source_builder import build_source_file_from_bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    """dbt-bq-sourcegen: Create or update BigQuery source YAML for dbt"""
    pass


@cli.command()
@click.option("--project-id", required=True, help="Google Cloud project ID")
@click.option(
    "--schema", "--dataset", required=True, help="BigQuery schema/dataset name"
)
@click.option(
    "--output", required=True, type=click.Path(), help="Output YAML file path"
)
@click.option("--table-pattern", help="Table name pattern (e.g., 'stg_*')")
@click.option("--exclude", help="Exclude tables containing this string")
@click.option("--sync-columns", is_flag=True, help="Sync column information")
@click.option(
    "--remove-deleted", is_flag=True, help="Remove tables/columns not in BigQuery"
)
def apply(
    project_id: str,
    schema: str,
    output: str,
    table_pattern: str,
    exclude: str,
    sync_columns: bool,
    remove_deleted: bool,
):
    """Create or update source YAML (auto-detects if file exists)"""
    output_path = Path(output)

    # Initialize clients
    bq_client = BigQueryClient(project_id)
    yaml_handler = YamlHandler()

    # Get schema from BigQuery
    bq_dataset = bq_client.get_dataset_schema(schema, table_pattern, exclude)

    if not bq_dataset.tables:
        logger.warning(f"No tables found in dataset {schema}")
        return

    if output_path.exists():
        logger.info(f"File exists, updating {output}")

        # Read existing file
        existing_file = yaml_handler.read_source_file(output)

        # Find or create source name
        source_name = schema
        if existing_file and existing_file.sources:
            for source in existing_file.sources:
                if source.schema_name == schema or source.name == schema:
                    source_name = source.name
                    break

        # Merge with existing
        merged_file = merge_source_file(
            bq_dataset.tables,
            existing_file,
            source_name,
            project_id,
            schema,
            remove_deleted_tables=remove_deleted,
            remove_deleted_columns=remove_deleted and sync_columns,
        )

        # Write updated file
        yaml_handler.write_source_file(output, merged_file)
        logger.info(f"Updated source file {output}")
    else:
        logger.info(f"File doesn't exist, creating {output}")

        # Build source file
        source_file = build_source_file_from_bigquery(bq_dataset)

        # Write to file
        yaml_handler.write_source_file(output, source_file)
        logger.info(f"Created source file at {output}")


if __name__ == "__main__":
    cli()
