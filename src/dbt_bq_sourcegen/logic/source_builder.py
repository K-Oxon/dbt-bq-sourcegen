"""Pure functions for building dbt source structures from BigQuery."""

from ..types.bigquery import BigQueryColumn, BigQueryDataset, BigQueryTable
from ..types.dbt import DbtColumn, DbtSource, DbtSourceFile, DbtTable


def build_column_from_bigquery(bq_column: BigQueryColumn) -> DbtColumn:
    """Build a dbt column from a BigQuery column.

    Args:
        bq_column: BigQuery column information.

    Returns:
        DbtColumn object.
    """
    return DbtColumn(
        name=bq_column.name,
        data_type=bq_column.field_type,
        description=bq_column.description or "",
    )


def build_table_from_bigquery(bq_table: BigQueryTable) -> DbtTable:
    """Build a dbt table from a BigQuery table.

    Args:
        bq_table: BigQuery table information.

    Returns:
        DbtTable object.
    """
    columns = [build_column_from_bigquery(col) for col in bq_table.columns]

    return DbtTable(
        name=bq_table.table_id,
        description=bq_table.description or "",
        columns=columns,
    )


def build_source_from_bigquery(
    bq_dataset: BigQueryDataset,
    source_name: str | None = None,
) -> DbtSource:
    """Build a dbt source from a BigQuery dataset.

    Args:
        bq_dataset: BigQuery dataset information.
        source_name: Optional source name (defaults to dataset_id).

    Returns:
        DbtSource object.
    """
    tables = [build_table_from_bigquery(table) for table in bq_dataset.tables]

    return DbtSource(
        name=source_name or bq_dataset.dataset_id,
        database=bq_dataset.project_id,
        schema=bq_dataset.dataset_id,
        description=bq_dataset.description or "",
        tables=tables,
    )


def build_source_file_from_bigquery(
    bq_dataset: BigQueryDataset,
    source_name: str | None = None,
) -> DbtSourceFile:
    """Build a complete dbt source file from a BigQuery dataset.

    Args:
        bq_dataset: BigQuery dataset information.
        source_name: Optional source name (defaults to dataset_id).

    Returns:
        DbtSourceFile object.
    """
    source = build_source_from_bigquery(bq_dataset, source_name)

    return DbtSourceFile(
        version=2,
        sources=[source],
    )
