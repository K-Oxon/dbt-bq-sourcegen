"""Pure functions for merging BigQuery and dbt source information."""

from typing import List, Optional

from ..types.bigquery import BigQueryColumn, BigQueryTable
from ..types.dbt import DbtColumn, DbtSource, DbtSourceFile, DbtTable
from .source_builder import build_column_from_bigquery, build_table_from_bigquery


def merge_columns(
    bq_columns: List[BigQueryColumn],
    yaml_columns: List[DbtColumn],
    remove_deleted: bool = False,
) -> List[DbtColumn]:
    """Merge BigQuery columns with existing YAML columns.

    Args:
        bq_columns: Columns from BigQuery.
        yaml_columns: Existing columns from YAML.
        remove_deleted: Whether to remove columns not in BigQuery.

    Returns:
        Merged list of DbtColumn objects.
    """
    bq_column_map = {col.name: col for col in bq_columns}
    yaml_column_map = {col.name: col for col in yaml_columns}

    merged_columns = []

    # Update existing columns and add new ones in BigQuery order
    for bq_col in bq_columns:
        if bq_col.name in yaml_column_map:
            # Update existing column
            yaml_col = yaml_column_map[bq_col.name]
            merged_col = DbtColumn(
                name=bq_col.name,
                data_type=bq_col.field_type,
                description=yaml_col.description or bq_col.description or "",
                meta=yaml_col.meta,
                tests=yaml_col.tests,
            )
        else:
            # Add new column
            merged_col = build_column_from_bigquery(bq_col)

        merged_columns.append(merged_col)

    # Keep columns that exist only in YAML (if not removing deleted)
    if not remove_deleted:
        for yaml_col in yaml_columns:
            if yaml_col.name not in bq_column_map:
                merged_columns.append(yaml_col)

    return merged_columns


def merge_table(
    bq_table: BigQueryTable,
    yaml_table: Optional[DbtTable],
    remove_deleted_columns: bool = False,
) -> DbtTable:
    """Merge a BigQuery table with an existing YAML table.

    Args:
        bq_table: Table from BigQuery.
        yaml_table: Existing table from YAML or None.
        remove_deleted_columns: Whether to remove columns not in BigQuery.

    Returns:
        Merged DbtTable object.
    """
    if not yaml_table:
        # Create new table from BigQuery
        return build_table_from_bigquery(bq_table)

    # Merge columns
    merged_columns = merge_columns(
        bq_table.columns,
        yaml_table.columns if yaml_table.columns else [],
        remove_deleted_columns,
    )

    # Keep existing description if present, otherwise use BigQuery's
    description = yaml_table.description or bq_table.description or ""

    return DbtTable(
        name=yaml_table.name,
        identifier=yaml_table.identifier,
        description=description,
        columns=merged_columns,
        meta=yaml_table.meta,
        tests=yaml_table.tests,
    )


def merge_sources(
    bq_tables: List[BigQueryTable],
    yaml_source: Optional[DbtSource],
    source_name: str,
    project_id: str,
    dataset_id: str,
    remove_deleted_tables: bool = False,
    remove_deleted_columns: bool = False,
) -> DbtSource:
    """Merge BigQuery tables with an existing dbt source.

    Args:
        bq_tables: Tables from BigQuery.
        yaml_source: Existing source from YAML or None.
        source_name: Name for the source.
        project_id: BigQuery project ID.
        dataset_id: BigQuery dataset ID.
        remove_deleted_tables: Whether to remove tables not in BigQuery.
        remove_deleted_columns: Whether to remove columns not in BigQuery.

    Returns:
        Merged DbtSource object.
    """
    if not yaml_source:
        # Create new source
        yaml_source = DbtSource(
            name=source_name,
            database=project_id,
            schema_name=dataset_id,
            tables=[],
        )

    bq_table_map = {table.table_id: table for table in bq_tables}
    yaml_table_map = {table.name: table for table in yaml_source.tables}

    merged_tables = []

    # Process tables in BigQuery order
    for bq_table in bq_tables:
        yaml_table = yaml_table_map.get(bq_table.table_id)
        merged_table = merge_table(bq_table, yaml_table, remove_deleted_columns)
        merged_tables.append(merged_table)

    # Keep tables that exist only in YAML (if not removing deleted)
    if not remove_deleted_tables:
        for yaml_table in yaml_source.tables:
            if yaml_table.name not in bq_table_map:
                merged_tables.append(yaml_table)

    return DbtSource(
        name=yaml_source.name,
        database=yaml_source.database or project_id,
        schema_name=yaml_source.schema_name or dataset_id,
        description=yaml_source.description,
        tables=merged_tables,
        meta=yaml_source.meta,
    )


def merge_source_file(
    bq_tables: List[BigQueryTable],
    yaml_file: Optional[DbtSourceFile],
    source_name: str,
    project_id: str,
    dataset_id: str,
    remove_deleted_tables: bool = False,
    remove_deleted_columns: bool = False,
) -> DbtSourceFile:
    """Merge BigQuery tables with an existing dbt source file.

    Args:
        bq_tables: Tables from BigQuery.
        yaml_file: Existing source file from YAML or None.
        source_name: Name for the source.
        project_id: BigQuery project ID.
        dataset_id: BigQuery dataset ID.
        remove_deleted_tables: Whether to remove tables not in BigQuery.
        remove_deleted_columns: Whether to remove columns not in BigQuery.

    Returns:
        Merged DbtSourceFile object.
    """
    if not yaml_file:
        yaml_file = DbtSourceFile(version=2, sources=[])

    # Find the matching source or create a new one
    yaml_source = None
    other_sources = []

    for source in yaml_file.sources:
        if source.name == source_name or source.schema_name == dataset_id:
            yaml_source = source
        else:
            other_sources.append(source)

    # Merge the source
    merged_source = merge_sources(
        bq_tables,
        yaml_source,
        source_name,
        project_id,
        dataset_id,
        remove_deleted_tables,
        remove_deleted_columns,
    )

    # Combine all sources
    all_sources = other_sources + [merged_source]

    return DbtSourceFile(
        version=yaml_file.version,
        sources=all_sources,
    )
