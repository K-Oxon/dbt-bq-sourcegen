"""Tests for merge strategy logic."""

from dbt_bq_sourcegen.types.bigquery import BigQueryColumn, BigQueryTable
from dbt_bq_sourcegen.types.dbt import DbtColumn, DbtTable, DbtSource, DbtSourceFile
from dbt_bq_sourcegen.logic.merge_strategy import (
    merge_columns,
    merge_table,
    merge_sources,
    merge_source_file,
)


class TestMergeColumns:
    """Tests for column merging."""

    def test_merge_columns_add_new(self):
        """Test adding new columns from BigQuery."""
        bq_columns = [
            BigQueryColumn(name="id", field_type="INT64"),
            BigQueryColumn(name="name", field_type="STRING", description="Name field"),
        ]
        yaml_columns = [
            DbtColumn(name="id", data_type="INT64", description="Primary key"),
        ]

        merged = merge_columns(bq_columns, yaml_columns)

        assert len(merged) == 2
        assert merged[0].name == "id"
        assert merged[0].description == "Primary key"  # Keep existing description
        assert merged[1].name == "name"
        assert merged[1].description == "Name field"  # Use BigQuery description

    def test_merge_columns_update_type(self):
        """Test updating column types from BigQuery."""
        bq_columns = [
            BigQueryColumn(name="id", field_type="STRING"),
        ]
        yaml_columns = [
            DbtColumn(name="id", data_type="INT64", description="ID field"),
        ]

        merged = merge_columns(bq_columns, yaml_columns)

        assert len(merged) == 1
        assert merged[0].name == "id"
        assert merged[0].data_type == "STRING"  # Updated from BigQuery
        assert merged[0].description == "ID field"  # Keep existing description

    def test_merge_columns_remove_deleted(self):
        """Test removing columns not in BigQuery."""
        bq_columns = [
            BigQueryColumn(name="id", field_type="INT64"),
        ]
        yaml_columns = [
            DbtColumn(name="id", data_type="INT64"),
            DbtColumn(name="deleted_col", data_type="STRING"),
        ]

        merged = merge_columns(bq_columns, yaml_columns, remove_deleted=True)

        assert len(merged) == 1
        assert merged[0].name == "id"

    def test_merge_columns_keep_deleted(self):
        """Test keeping columns not in BigQuery."""
        bq_columns = [
            BigQueryColumn(name="id", field_type="INT64"),
        ]
        yaml_columns = [
            DbtColumn(name="id", data_type="INT64"),
            DbtColumn(name="yaml_only", data_type="STRING"),
        ]

        merged = merge_columns(bq_columns, yaml_columns, remove_deleted=False)

        assert len(merged) == 2
        assert merged[0].name == "id"
        assert merged[1].name == "yaml_only"


class TestMergeTable:
    """Tests for table merging."""

    def test_merge_table_new(self):
        """Test merging when YAML table doesn't exist."""
        bq_table = BigQueryTable(
            dataset_id="test",
            table_id="new_table",
            description="New table",
            columns=[BigQueryColumn(name="id", field_type="INT64")],
        )

        merged = merge_table(bq_table, None)

        assert merged.name == "new_table"
        assert merged.description == "New table"
        assert len(merged.columns) == 1

    def test_merge_table_existing(self):
        """Test merging with existing YAML table."""
        bq_table = BigQueryTable(
            dataset_id="test",
            table_id="test_table",
            description="BQ description",
            columns=[
                BigQueryColumn(name="id", field_type="INT64"),
                BigQueryColumn(name="new_col", field_type="STRING"),
            ],
        )

        yaml_table = DbtTable(
            name="test_table",
            description="YAML description",
            columns=[
                DbtColumn(name="id", data_type="INT64", description="Primary key"),
            ],
        )

        merged = merge_table(bq_table, yaml_table)

        assert merged.name == "test_table"
        assert merged.description == "YAML description"  # Keep existing
        assert len(merged.columns) == 2
        assert merged.columns[0].description == "Primary key"


class TestMergeSources:
    """Tests for source merging."""

    def test_merge_sources_new(self):
        """Test creating new source."""
        bq_tables = [
            BigQueryTable(
                dataset_id="test",
                table_id="table1",
                columns=[],
            ),
        ]

        merged = merge_sources(
            bq_tables,
            None,
            "test_source",
            "test_project",
            "test_dataset",
        )

        assert merged.name == "test_source"
        assert merged.database == "test_project"
        assert merged.schema_name == "test_dataset"
        assert len(merged.tables) == 1

    def test_merge_sources_existing(self):
        """Test merging with existing source."""
        bq_tables = [
            BigQueryTable(
                dataset_id="test",
                table_id="table1",
                columns=[],
            ),
            BigQueryTable(
                dataset_id="test",
                table_id="table2",
                columns=[],
            ),
        ]

        yaml_source = DbtSource(
            name="test_source",
            tables=[
                DbtTable(name="table1"),
                DbtTable(name="table3"),  # Only in YAML
            ],
        )

        merged = merge_sources(
            bq_tables,
            yaml_source,
            "test_source",
            "test_project",
            "test_dataset",
            remove_deleted_tables=False,
        )

        assert len(merged.tables) == 3  # table1, table2, table3
        table_names = [t.name for t in merged.tables]
        assert "table1" in table_names
        assert "table2" in table_names
        assert "table3" in table_names


class TestMergeSourceFile:
    """Tests for source file merging."""

    def test_merge_source_file_new(self):
        """Test creating new source file."""
        bq_tables = [
            BigQueryTable(dataset_id="test", table_id="table1", columns=[]),
        ]

        merged = merge_source_file(
            bq_tables,
            None,
            "test_source",
            "test_project",
            "test_dataset",
        )

        assert merged.version == 2
        assert len(merged.sources) == 1
        assert merged.sources[0].name == "test_source"

    def test_merge_source_file_existing(self):
        """Test merging with existing source file."""
        bq_tables = [
            BigQueryTable(dataset_id="test", table_id="table1", columns=[]),
        ]

        yaml_file = DbtSourceFile(
            version=2,
            sources=[
                DbtSource(name="test_source", schema_name="test_dataset", tables=[]),
                DbtSource(name="other_source", schema_name="other_dataset", tables=[]),
            ],
        )

        merged = merge_source_file(
            bq_tables,
            yaml_file,
            "test_source",
            "test_project",
            "test_dataset",
        )

        assert len(merged.sources) == 2  # Both sources preserved
        assert merged.sources[0].name == "other_source"  # Other source preserved
        assert merged.sources[1].name == "test_source"  # Updated source
        assert len(merged.sources[1].tables) == 1
