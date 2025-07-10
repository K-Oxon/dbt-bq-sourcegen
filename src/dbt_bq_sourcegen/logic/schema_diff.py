"""Pure functions for calculating schema differences."""

from typing import List, Optional

from ..types.bigquery import BigQueryColumn, BigQueryTable
from ..types.dbt import DbtColumn, DbtSource, DbtTable
from ..types.diff import ColumnDiff, SchemaDiff, TableDiff


def calculate_column_diff(
    bq_columns: List[BigQueryColumn],
    yaml_columns: List[DbtColumn],
) -> ColumnDiff:
    """Calculate differences between BigQuery and YAML columns.

    Args:
        bq_columns: Columns from BigQuery.
        yaml_columns: Columns from dbt YAML.

    Returns:
        ColumnDiff object with added, removed, modified, and type_changed columns.
    """
    bq_column_map = {col.name: col for col in bq_columns}
    yaml_column_map = {col.name: col for col in yaml_columns}

    bq_names = set(bq_column_map.keys())
    yaml_names = set(yaml_column_map.keys())

    added = bq_names - yaml_names
    removed = yaml_names - bq_names
    common = bq_names & yaml_names

    modified = set()
    type_changed = set()

    for name in common:
        bq_col = bq_column_map[name]
        yaml_col = yaml_column_map[name]

        # Check if description needs update (only if YAML description is empty)
        if not yaml_col.description and bq_col.description:
            modified.add(name)

        # Check if data type changed
        if yaml_col.data_type and yaml_col.data_type != bq_col.field_type:
            type_changed.add(name)

    return ColumnDiff(
        added=added,
        removed=removed,
        modified=modified,
        type_changed=type_changed,
    )


def calculate_table_diff(
    bq_table: Optional[BigQueryTable],
    yaml_table: Optional[DbtTable],
) -> TableDiff:
    """Calculate differences between a BigQuery table and YAML table.

    Args:
        bq_table: BigQuery table or None if doesn't exist.
        yaml_table: dbt YAML table or None if doesn't exist.

    Returns:
        TableDiff object with existence flags and column differences.
    """
    if not bq_table and not yaml_table:
        raise ValueError("Both tables cannot be None")

    table_name = yaml_table.name if yaml_table else bq_table.table_id

    # Check existence
    exists_in_bigquery = bq_table is not None
    exists_in_yaml = yaml_table is not None

    # Calculate column diff if both exist
    column_diff = None
    description_changed = False

    if exists_in_bigquery and exists_in_yaml:
        column_diff = calculate_column_diff(
            bq_table.columns,
            yaml_table.columns if yaml_table.columns else [],
        )

        # Check if description needs update
        if not yaml_table.description and bq_table.description:
            description_changed = True

    return TableDiff(
        table_name=table_name,
        exists_in_bigquery=exists_in_bigquery,
        exists_in_yaml=exists_in_yaml,
        column_diff=column_diff,
        description_changed=description_changed,
    )


def calculate_schema_diff(
    bq_tables: List[BigQueryTable],
    yaml_source: Optional[DbtSource],
) -> SchemaDiff:
    """Calculate complete schema differences between BigQuery and YAML.

    Args:
        bq_tables: List of BigQuery tables.
        yaml_source: dbt source from YAML or None if doesn't exist.

    Returns:
        SchemaDiff object with all differences.
    """
    yaml_tables = yaml_source.tables if yaml_source else []
    yaml_table_map = {t.name: t for t in yaml_tables}
    bq_table_map = {t.table_id: t for t in bq_tables}

    # Find added tables (in BigQuery but not in YAML)
    yaml_table_names = set(yaml_table_map.keys())
    bq_table_names = set(bq_table_map.keys())

    tables_added = list(bq_table_names - yaml_table_names)
    tables_removed = list(yaml_table_names - bq_table_names)

    # Calculate diffs for all tables
    table_diffs = []

    # Process tables that exist in both
    for table_name in yaml_table_names & bq_table_names:
        diff = calculate_table_diff(
            bq_table_map[table_name],
            yaml_table_map[table_name],
        )
        table_diffs.append(diff)

    # Process added tables
    for table_name in tables_added:
        diff = calculate_table_diff(
            bq_table_map[table_name],
            None,
        )
        table_diffs.append(diff)

    # Process removed tables
    for table_name in tables_removed:
        diff = calculate_table_diff(
            None,
            yaml_table_map[table_name],
        )
        table_diffs.append(diff)

    return SchemaDiff(
        tables_added=tables_added,
        tables_removed=tables_removed,
        table_diffs=table_diffs,
    )
