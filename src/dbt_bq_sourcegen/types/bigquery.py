"""BigQuery related type definitions."""

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class BigQueryColumn(BaseModel):
    """Represents a column in a BigQuery table."""

    name: str
    field_type: str
    mode: str = "NULLABLE"
    description: Optional[str] = None

    model_config = ConfigDict(frozen=True)


class BigQueryTable(BaseModel):
    """Represents a BigQuery table."""

    dataset_id: str
    table_id: str
    description: Optional[str] = None
    columns: List[BigQueryColumn] = Field(default_factory=list)

    model_config = ConfigDict(frozen=True)


class BigQueryDataset(BaseModel):
    """Represents a BigQuery dataset."""

    project_id: str
    dataset_id: str
    description: Optional[str] = None
    tables: List[BigQueryTable] = Field(default_factory=list)

    model_config = ConfigDict(frozen=True)
