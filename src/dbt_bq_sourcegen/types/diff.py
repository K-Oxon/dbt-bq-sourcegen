"""Schema diff related type definitions."""

from typing import List, Optional, Set

from pydantic import BaseModel, ConfigDict, Field


class ColumnDiff(BaseModel):
    """Represents column differences between BigQuery and dbt YAML."""

    added: Set[str] = Field(default_factory=set)
    removed: Set[str] = Field(default_factory=set)
    modified: Set[str] = Field(default_factory=set)
    type_changed: Set[str] = Field(default_factory=set)

    model_config = ConfigDict(frozen=True)


class TableDiff(BaseModel):
    """Represents table differences between BigQuery and dbt YAML."""

    table_name: str
    exists_in_bigquery: bool = True
    exists_in_yaml: bool = True
    column_diff: Optional[ColumnDiff] = None
    description_changed: bool = False

    model_config = ConfigDict(frozen=True)


class SchemaDiff(BaseModel):
    """Represents complete schema differences between BigQuery and dbt YAML."""

    tables_added: List[str] = Field(default_factory=list)
    tables_removed: List[str] = Field(default_factory=list)
    table_diffs: List[TableDiff] = Field(default_factory=list)

    model_config = ConfigDict(frozen=True)
