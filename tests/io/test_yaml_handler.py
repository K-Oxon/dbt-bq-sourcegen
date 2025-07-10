"""Tests for YAML handler."""

import tempfile
from pathlib import Path

from dbt_bq_sourcegen.io.yaml_handler import YamlHandler
from dbt_bq_sourcegen.types.dbt import DbtColumn, DbtTable, DbtSource, DbtSourceFile


class TestYamlHandler:
    """Tests for YamlHandler."""

    def setup_method(self):
        """Set up test method."""
        self.yaml_handler = YamlHandler()

    def test_write_and_read_source_file(self):
        """Test writing and reading a source file."""
        # Create test data
        columns = [
            DbtColumn(name="id", data_type="INT64", description="Primary key"),
            DbtColumn(name="name", data_type="STRING", description="Name field"),
        ]

        table = DbtTable(
            name="test_table",
            description="Test table description",
            columns=columns,
        )

        source = DbtSource(
            name="test_source",
            database="test_project",
            schema="test_dataset",
            description="Test source description",
            tables=[table],
        )

        source_file = DbtSourceFile(
            version=2,
            sources=[source],
        )

        # Write to temporary file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            temp_path = f.name

        try:
            # Write file
            self.yaml_handler.write_source_file(temp_path, source_file)

            # Read file back
            read_file = self.yaml_handler.read_source_file(temp_path)

            # Verify structure
            assert read_file is not None
            assert read_file.version == 2
            assert len(read_file.sources) == 1

            read_source = read_file.sources[0]
            assert read_source.name == "test_source"
            assert read_source.database == "test_project"
            assert read_source.schema_name == "test_dataset"
            assert read_source.description == "Test source description"
            assert len(read_source.tables) == 1

            read_table = read_source.tables[0]
            assert read_table.name == "test_table"
            assert read_table.description == "Test table description"
            assert len(read_table.columns) == 2

            assert read_table.columns[0].name == "id"
            assert read_table.columns[0].data_type == "INT64"
            assert read_table.columns[0].description == "Primary key"

        finally:
            # Clean up
            Path(temp_path).unlink()

    def test_read_nonexistent_file(self):
        """Test reading a non-existent file."""
        result = self.yaml_handler.read_source_file("/nonexistent/file.yml")
        assert result is None

    def test_preserve_formatting(self):
        """Test that YAML formatting is preserved."""
        source_file = DbtSourceFile(
            version=2,
            sources=[
                DbtSource(
                    name="test",
                    schema="test_schema",
                    tables=[
                        DbtTable(
                            name="table1",
                            columns=[
                                DbtColumn(name="col1", description="Test column"),
                            ],
                        ),
                    ],
                ),
            ],
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            temp_path = f.name

        try:
            # Write file
            self.yaml_handler.write_source_file(temp_path, source_file)

            # Read raw content
            with open(temp_path, "r") as f:
                content = f.read()

            # Check formatting
            assert "version: 2" in content
            assert "sources:" in content
            assert "  - name: test" in content  # 2-space indent
            assert "    tables:" in content  # 4-space indent for nested

        finally:
            Path(temp_path).unlink()

    def test_empty_columns_handling(self):
        """Test handling of tables with no columns."""
        source_file = DbtSourceFile(
            version=2,
            sources=[
                DbtSource(
                    name="test",
                    tables=[
                        DbtTable(name="empty_table", columns=[]),
                    ],
                ),
            ],
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            temp_path = f.name

        try:
            self.yaml_handler.write_source_file(temp_path, source_file)
            read_file = self.yaml_handler.read_source_file(temp_path)

            assert read_file is not None
            assert len(read_file.sources[0].tables[0].columns) == 0

        finally:
            Path(temp_path).unlink()
