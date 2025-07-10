"""Type definitions for dbt-bq-sourcegen."""

from .bigquery import BigQueryColumn, BigQueryDataset, BigQueryTable
from .dbt import DbtColumn, DbtSource, DbtSourceFile, DbtTable
from .diff import ColumnDiff, SchemaDiff, TableDiff

__all__ = [
    "BigQueryColumn",
    "BigQueryTable",
    "BigQueryDataset",
    "DbtColumn",
    "DbtTable",
    "DbtSource",
    "DbtSourceFile",
    "ColumnDiff",
    "TableDiff",
    "SchemaDiff",
]
