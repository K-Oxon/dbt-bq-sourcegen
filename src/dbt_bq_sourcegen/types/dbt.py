"""dbt source YAML related type definitions."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class DbtColumn(BaseModel):
    """Represents a column in a dbt source YAML."""

    name: str
    data_type: Optional[str] = None
    description: Optional[str] = ""
    meta: Optional[Dict[str, Any]] = None
    tests: Optional[List[Any]] = None
    quote: Optional[bool] = None
    tags: Optional[List[str]] = None

    model_config = ConfigDict(frozen=True, extra="allow")


class DbtTable(BaseModel):
    """Represents a table in a dbt source YAML."""

    name: str
    identifier: Optional[str] = None
    description: Optional[str] = ""
    columns: Optional[List[DbtColumn]] = Field(default_factory=list)
    meta: Optional[Dict[str, Any]] = None
    tests: Optional[List[Any]] = None
    loaded_at_field: Optional[str] = None
    tags: Optional[List[str]] = None
    config: Optional[Dict[str, Any]] = None
    quoting: Optional[Dict[str, bool]] = None
    external: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(frozen=True, extra="allow")


class DbtSource(BaseModel):
    """Represents a source in a dbt source YAML."""

    name: str
    database: Optional[str] = None
    schema_name: Optional[str] = Field(None, alias="schema")
    description: Optional[str] = ""
    tables: List[DbtTable] = Field(default_factory=list)
    meta: Optional[Dict[str, Any]] = None
    loader: Optional[str] = None
    loaded_at_field: Optional[str] = None
    tags: Optional[List[str]] = None
    config: Optional[Dict[str, Any]] = None
    quoting: Optional[Dict[str, bool]] = None
    overrides: Optional[str] = None

    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="allow")


class DbtSourceFile(BaseModel):
    """Represents a complete dbt source YAML file."""

    version: int = 2
    sources: List[DbtSource] = Field(default_factory=list)

    model_config = ConfigDict(frozen=True, extra="allow")
