"""YAML file handler for dbt source files."""

import logging
from pathlib import Path
from typing import Any, Dict, Optional

import ruamel.yaml

from ..types.dbt import DbtColumn, DbtSource, DbtSourceFile, DbtTable

logger = logging.getLogger(__name__)


class YamlHandler:
    """Handler for reading and writing dbt source YAML files."""

    def __init__(self):
        """Initialize YAML handler with ruamel.yaml configuration."""
        self.yaml = ruamel.yaml.YAML(typ="rt")
        self.yaml.indent(mapping=2, sequence=4, offset=2)
        self.yaml.preserve_quotes = True
        self.yaml.default_flow_style = False
        self.yaml.allow_unicode = True
        self.yaml.sort_keys = False
        self.yaml.width = 4096

    def read_source_file(self, file_path: str) -> Optional[DbtSourceFile]:
        """Read a dbt source YAML file.

        Args:
            file_path: Path to the source YAML file.

        Returns:
            DbtSourceFile object or None if file doesn't exist.
        """
        path = Path(file_path)
        if not path.exists():
            return None

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = self.yaml.load(f)

            return self._parse_source_file(data)
        except Exception as e:
            logger.error(f"Failed to read source file {file_path}: {e}")
            raise

    def write_source_file(self, file_path: str, source_file: DbtSourceFile) -> None:
        """Write a dbt source YAML file.

        Args:
            file_path: Path to write the source YAML file.
            source_file: DbtSourceFile object to write.
        """
        data = self._serialize_source_file(source_file)

        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                self.yaml.dump(data, f)
        except Exception as e:
            logger.error(f"Failed to write source file {file_path}: {e}")
            raise

    def _parse_source_file(self, data: Dict[str, Any]) -> DbtSourceFile:
        """Parse raw YAML data into DbtSourceFile.

        Args:
            data: Raw YAML data.

        Returns:
            Parsed DbtSourceFile object.
        """
        sources = []

        for source_data in data.get("sources", []):
            tables = []

            for table_data in source_data.get("tables", []):
                columns = []

                if table_data.get("columns"):
                    for column_data in table_data["columns"]:
                        column = DbtColumn(
                            name=column_data["name"],
                            data_type=column_data.get("data_type"),
                            description=column_data.get("description", ""),
                            meta=column_data.get("meta"),
                            tests=column_data.get("tests"),
                        )
                        columns.append(column)

                table = DbtTable(
                    name=table_data["name"],
                    identifier=table_data.get("identifier"),
                    description=table_data.get("description", ""),
                    columns=columns,
                    meta=table_data.get("meta"),
                    tests=table_data.get("tests"),
                )
                tables.append(table)

            source = DbtSource(
                name=source_data["name"],
                database=source_data.get("database"),
                schema_name=source_data.get("schema"),
                description=source_data.get("description", ""),
                tables=tables,
                meta=source_data.get("meta"),
            )
            sources.append(source)

        return DbtSourceFile(
            version=data.get("version", 2),
            sources=sources,
        )

    def _serialize_source_file(self, source_file: DbtSourceFile) -> Dict[str, Any]:
        """Serialize DbtSourceFile into raw YAML data.

        Args:
            source_file: DbtSourceFile object to serialize.

        Returns:
            Raw YAML data.
        """
        data = {
            "version": source_file.version,
            "sources": [],
        }

        for source in source_file.sources:
            source_data = {
                "name": source.name,
            }

            if source.database:
                source_data["database"] = source.database
            if source.schema_name:
                source_data["schema"] = source.schema_name
            if source.description:
                source_data["description"] = source.description
            if source.meta:
                source_data["meta"] = source.meta

            source_data["tables"] = []

            for table in source.tables:
                table_data = {
                    "name": table.name,
                }

                if table.identifier:
                    table_data["identifier"] = table.identifier
                if table.description:
                    table_data["description"] = table.description
                if table.meta:
                    table_data["meta"] = table.meta
                if table.tests:
                    table_data["tests"] = table.tests

                if table.columns:
                    table_data["columns"] = []

                    for column in table.columns:
                        column_data = {
                            "name": column.name,
                        }

                        if column.data_type:
                            column_data["data_type"] = column.data_type
                        if column.description:
                            column_data["description"] = column.description
                        if column.meta:
                            column_data["meta"] = column.meta
                        if column.tests:
                            column_data["tests"] = column.tests

                        table_data["columns"].append(column_data)

                source_data["tables"].append(table_data)

            data["sources"].append(source_data)

        return data
