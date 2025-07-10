"""YAML file handler for dbt source files."""

from pathlib import Path
from typing import Any, Dict, Optional

import ruamel.yaml
from loguru import logger
from ruamel.yaml.scalarstring import DoubleQuotedScalarString, LiteralScalarString

from ..types.dbt import DbtColumn, DbtSource, DbtSourceFile, DbtTable


class YamlHandler:
    """Handler for reading and writing dbt source YAML files."""

    def __init__(self):
        """Initialize YAML handler with ruamel.yaml configuration."""
        self.yaml = ruamel.yaml.YAML(typ="rt")
        self.yaml.indent(mapping=2, sequence=4, offset=2)
        self.yaml.preserve_quotes = True
        self.yaml.default_flow_style = False
        self.yaml.allow_unicode = True
        # self.yaml.sort_keys = False  # Not available in ruamel.yaml rt mode
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
                # Write version with empty line after it
                f.write(f"version: {data['version']}\n\n")
                # Write the rest of the data
                remaining_data = {"sources": data["sources"]}
                self.yaml.dump(remaining_data, f)
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
                            quote=column_data.get("quote"),
                            tags=column_data.get("tags"),
                        )
                        columns.append(column)

                table = DbtTable(
                    name=table_data["name"],
                    identifier=table_data.get("identifier"),
                    description=table_data.get("description", ""),
                    columns=columns,
                    meta=table_data.get("meta"),
                    tests=table_data.get("tests"),
                    loaded_at_field=table_data.get("loaded_at_field"),
                    tags=table_data.get("tags"),
                    config=table_data.get("config"),
                    quoting=table_data.get("quoting"),
                    external=table_data.get("external"),
                )
                tables.append(table)

            source = DbtSource(
                name=source_data["name"],
                database=source_data.get("database"),
                schema=source_data.get("schema"),
                description=source_data.get("description", ""),
                tables=tables,
                meta=source_data.get("meta"),
                loader=source_data.get("loader"),
                loaded_at_field=source_data.get("loaded_at_field"),
                tags=source_data.get("tags"),
                config=source_data.get("config"),
                quoting=source_data.get("quoting"),
                overrides=source_data.get("overrides"),
            )
            sources.append(source)

        return DbtSourceFile(
            version=data.get("version", 2),
            sources=sources,
        )

    def _format_description(self, description: str) -> Any:
        """Format description for YAML output.

        Args:
            description: Description text.

        Returns:
            Empty string or LiteralScalarString for block notation.
        """
        if not description:
            return DoubleQuotedScalarString("")
        return LiteralScalarString(description)

    def _serialize_source_file(self, source_file: DbtSourceFile) -> Dict[str, Any]:
        """Serialize DbtSourceFile into raw YAML data.

        Args:
            source_file: DbtSourceFile object to serialize.

        Returns:
            Raw YAML data.
        """
        data: Dict[str, Any] = {
            "version": source_file.version,
            "sources": [],
        }

        for source in source_file.sources:
            source_data: Dict[str, Any] = {
                "name": source.name,
            }

            if source.database:
                source_data["database"] = source.database
            if source.schema_name:
                source_data["schema"] = source.schema_name
            # Always include description with proper formatting
            source_data["description"] = self._format_description(
                source.description or ""
            )
            if source.meta:
                source_data["meta"] = source.meta
            if source.loader:
                source_data["loader"] = source.loader
            if source.loaded_at_field:
                source_data["loaded_at_field"] = source.loaded_at_field
            if source.tags:
                source_data["tags"] = source.tags
            if source.config:
                source_data["config"] = source.config
            if source.quoting:
                source_data["quoting"] = source.quoting
            if source.overrides:
                source_data["overrides"] = source.overrides

            source_data["tables"] = []

            for table in source.tables:
                table_data: Dict[str, Any] = {
                    "name": table.name,
                }

                if table.identifier:
                    table_data["identifier"] = table.identifier
                # Always include description with proper formatting
                table_data["description"] = self._format_description(
                    table.description or ""
                )
                if table.meta:
                    table_data["meta"] = table.meta
                if table.tests:
                    table_data["tests"] = table.tests
                if table.loaded_at_field:
                    table_data["loaded_at_field"] = table.loaded_at_field
                if table.tags:
                    table_data["tags"] = table.tags
                if table.config:
                    table_data["config"] = table.config
                if table.quoting:
                    table_data["quoting"] = table.quoting
                if table.external:
                    table_data["external"] = table.external

                if table.columns:
                    table_data["columns"] = []

                    for column in table.columns:
                        column_data: Dict[str, Any] = {
                            "name": column.name,
                        }

                        if column.data_type:
                            column_data["data_type"] = column.data_type
                        # Always include description with proper formatting
                        column_data["description"] = self._format_description(
                            column.description or ""
                        )
                        if column.meta:
                            column_data["meta"] = column.meta
                        if column.tests:
                            column_data["tests"] = column.tests
                        if column.quote is not None:
                            column_data["quote"] = column.quote
                        if column.tags:
                            column_data["tags"] = column.tags

                        table_data["columns"].append(column_data)

                source_data["tables"].append(table_data)

            data["sources"].append(source_data)

        return data
