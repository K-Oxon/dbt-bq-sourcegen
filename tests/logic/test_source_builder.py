"""Tests for source builder logic."""

from dbt_bq_sourcegen.types.bigquery import (
    BigQueryColumn,
    BigQueryTable,
    BigQueryDataset,
)
from dbt_bq_sourcegen.logic.source_builder import (
    build_column_from_bigquery,
    build_table_from_bigquery,
    build_source_from_bigquery,
    build_source_file_from_bigquery,
)


class TestSourceBuilder:
    """Tests for source builder functions."""

    def test_build_column_from_bigquery(self):
        """Test building dbt column from BigQuery column."""
        bq_column = BigQueryColumn(
            name="test_col",
            field_type="STRING",
            mode="NULLABLE",
            description="Test description",
        )

        dbt_column = build_column_from_bigquery(bq_column)

        assert dbt_column.name == "test_col"
        assert dbt_column.data_type == "STRING"
        assert dbt_column.description == "Test description"

    def test_build_column_from_bigquery_no_description(self):
        """Test building dbt column when BigQuery column has no description."""
        bq_column = BigQueryColumn(
            name="test_col",
            field_type="INT64",
        )

        dbt_column = build_column_from_bigquery(bq_column)

        assert dbt_column.name == "test_col"
        assert dbt_column.data_type == "INT64"
        assert dbt_column.description == ""

    def test_build_table_from_bigquery(self):
        """Test building dbt table from BigQuery table."""
        bq_columns = [
            BigQueryColumn(name="id", field_type="INT64", description="Primary key"),
            BigQueryColumn(name="name", field_type="STRING"),
        ]

        bq_table = BigQueryTable(
            dataset_id="test_dataset",
            table_id="test_table",
            description="Test table description",
            columns=bq_columns,
        )

        dbt_table = build_table_from_bigquery(bq_table)

        assert dbt_table.name == "test_table"
        assert dbt_table.description == "Test table description"
        assert len(dbt_table.columns) == 2
        assert dbt_table.columns[0].name == "id"
        assert dbt_table.columns[0].description == "Primary key"
        assert dbt_table.columns[1].name == "name"
        assert dbt_table.columns[1].description == ""

    def test_build_source_from_bigquery(self):
        """Test building dbt source from BigQuery dataset."""
        bq_tables = [
            BigQueryTable(
                dataset_id="test_dataset",
                table_id="table1",
                columns=[BigQueryColumn(name="id", field_type="INT64")],
            ),
            BigQueryTable(
                dataset_id="test_dataset",
                table_id="table2",
                columns=[],
            ),
        ]

        bq_dataset = BigQueryDataset(
            project_id="test_project",
            dataset_id="test_dataset",
            description="Test dataset",
            tables=bq_tables,
        )

        dbt_source = build_source_from_bigquery(bq_dataset)

        assert dbt_source.name == "test_dataset"
        assert dbt_source.database == "test_project"
        assert dbt_source.schema_name == "test_dataset"
        assert dbt_source.description == "Test dataset"
        assert len(dbt_source.tables) == 2

    def test_build_source_from_bigquery_custom_name(self):
        """Test building dbt source with custom name."""
        bq_dataset = BigQueryDataset(
            project_id="test_project",
            dataset_id="test_dataset",
            tables=[],
        )

        dbt_source = build_source_from_bigquery(bq_dataset, source_name="custom_name")

        assert dbt_source.name == "custom_name"
        assert dbt_source.schema_name == "test_dataset"

    def test_build_source_file_from_bigquery(self):
        """Test building complete source file from BigQuery dataset."""
        bq_dataset = BigQueryDataset(
            project_id="test_project",
            dataset_id="test_dataset",
            tables=[
                BigQueryTable(
                    dataset_id="test_dataset",
                    table_id="table1",
                    columns=[],
                ),
            ],
        )

        source_file = build_source_file_from_bigquery(bq_dataset)

        assert source_file.version == 2
        assert len(source_file.sources) == 1
        assert source_file.sources[0].name == "test_dataset"
        assert len(source_file.sources[0].tables) == 1
