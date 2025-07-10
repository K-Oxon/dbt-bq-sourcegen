"""Tests for type definitions."""

import pytest
from dbt_bq_sourcegen.types.bigquery import (
    BigQueryColumn,
    BigQueryTable,
    BigQueryDataset,
)
from dbt_bq_sourcegen.types.dbt import DbtColumn, DbtTable, DbtSource, DbtSourceFile
from dbt_bq_sourcegen.types.diff import ColumnDiff, TableDiff, SchemaDiff


class TestBigQueryTypes:
    """Tests for BigQuery type definitions."""

    def test_bigquery_column(self):
        """Test BigQueryColumn creation and immutability."""
        column = BigQueryColumn(
            name="test_column",
            field_type="STRING",
            mode="NULLABLE",
            description="Test description",
        )

        assert column.name == "test_column"
        assert column.field_type == "STRING"
        assert column.mode == "NULLABLE"
        assert column.description == "Test description"

        # Test immutability
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            column.name = "new_name"

    def test_bigquery_table(self):
        """Test BigQueryTable creation."""
        columns = [
            BigQueryColumn(name="id", field_type="INT64"),
            BigQueryColumn(name="name", field_type="STRING"),
        ]

        table = BigQueryTable(
            dataset_id="test_dataset",
            table_id="test_table",
            description="Test table",
            columns=columns,
        )

        assert table.dataset_id == "test_dataset"
        assert table.table_id == "test_table"
        assert table.description == "Test table"
        assert len(table.columns) == 2

    def test_bigquery_dataset(self):
        """Test BigQueryDataset creation."""
        dataset = BigQueryDataset(
            project_id="test_project",
            dataset_id="test_dataset",
            tables=[],
        )

        assert dataset.project_id == "test_project"
        assert dataset.dataset_id == "test_dataset"
        assert dataset.tables == []


class TestDbtTypes:
    """Tests for dbt type definitions."""

    def test_dbt_column(self):
        """Test DbtColumn creation."""
        column = DbtColumn(
            name="test_column",
            data_type="STRING",
            description="Test description",
        )

        assert column.name == "test_column"
        assert column.data_type == "STRING"
        assert column.description == "Test description"
        assert column.meta is None
        assert column.tests is None

    def test_dbt_table(self):
        """Test DbtTable creation."""
        columns = [
            DbtColumn(name="id", data_type="INT64"),
            DbtColumn(name="name", data_type="STRING"),
        ]

        table = DbtTable(
            name="test_table",
            description="Test table",
            columns=columns,
        )

        assert table.name == "test_table"
        assert table.description == "Test table"
        assert len(table.columns) == 2
        assert table.identifier is None

    def test_dbt_source(self):
        """Test DbtSource creation."""
        source = DbtSource(
            name="test_source",
            database="test_project",
            schema="test_dataset",
            description="Test source",
            tables=[],
        )

        assert source.name == "test_source"
        assert source.database == "test_project"
        assert source.schema_name == "test_dataset"
        assert source.description == "Test source"
        assert source.tables == []

    def test_dbt_source_file(self):
        """Test DbtSourceFile creation."""
        source_file = DbtSourceFile(
            version=2,
            sources=[],
        )

        assert source_file.version == 2
        assert source_file.sources == []


class TestDiffTypes:
    """Tests for diff type definitions."""

    def test_column_diff(self):
        """Test ColumnDiff creation."""
        diff = ColumnDiff(
            added={"new_col"},
            removed={"old_col"},
            modified={"changed_col"},
            type_changed={"type_col"},
        )

        assert diff.added == {"new_col"}
        assert diff.removed == {"old_col"}
        assert diff.modified == {"changed_col"}
        assert diff.type_changed == {"type_col"}

    def test_table_diff(self):
        """Test TableDiff creation."""
        column_diff = ColumnDiff()

        diff = TableDiff(
            table_name="test_table",
            exists_in_bigquery=True,
            exists_in_yaml=False,
            column_diff=column_diff,
            description_changed=True,
        )

        assert diff.table_name == "test_table"
        assert diff.exists_in_bigquery is True
        assert diff.exists_in_yaml is False
        assert diff.column_diff == column_diff
        assert diff.description_changed is True

    def test_schema_diff(self):
        """Test SchemaDiff creation."""
        diff = SchemaDiff(
            tables_added=["new_table"],
            tables_removed=["old_table"],
            table_diffs=[],
        )

        assert diff.tables_added == ["new_table"]
        assert diff.tables_removed == ["old_table"]
        assert diff.table_diffs == []
