"""Tests for schema diff logic."""

from dbt_bq_sourcegen.types.bigquery import BigQueryColumn, BigQueryTable
from dbt_bq_sourcegen.types.dbt import DbtColumn, DbtTable, DbtSource
from dbt_bq_sourcegen.logic.schema_diff import (
    calculate_column_diff,
    calculate_table_diff,
    calculate_schema_diff,
)


class TestColumnDiff:
    """Tests for column diff calculation."""

    def test_calculate_column_diff_added(self):
        """Test detecting added columns."""
        bq_columns = [
            BigQueryColumn(name="id", field_type="INT64"),
            BigQueryColumn(name="name", field_type="STRING"),
            BigQueryColumn(name="email", field_type="STRING"),
        ]
        yaml_columns = [
            DbtColumn(name="id", data_type="INT64"),
            DbtColumn(name="name", data_type="STRING"),
        ]

        diff = calculate_column_diff(bq_columns, yaml_columns)

        assert diff.added == {"email"}
        assert diff.removed == set()
        assert diff.modified == set()
        assert diff.type_changed == set()

    def test_calculate_column_diff_removed(self):
        """Test detecting removed columns."""
        bq_columns = [
            BigQueryColumn(name="id", field_type="INT64"),
        ]
        yaml_columns = [
            DbtColumn(name="id", data_type="INT64"),
            DbtColumn(name="deleted_col", data_type="STRING"),
        ]

        diff = calculate_column_diff(bq_columns, yaml_columns)

        assert diff.added == set()
        assert diff.removed == {"deleted_col"}
        assert diff.modified == set()
        assert diff.type_changed == set()

    def test_calculate_column_diff_modified(self):
        """Test detecting modified columns (description update)."""
        bq_columns = [
            BigQueryColumn(name="id", field_type="INT64", description="Primary key"),
        ]
        yaml_columns = [
            DbtColumn(name="id", data_type="INT64", description=""),
        ]

        diff = calculate_column_diff(bq_columns, yaml_columns)

        assert diff.added == set()
        assert diff.removed == set()
        assert diff.modified == {"id"}
        assert diff.type_changed == set()

    def test_calculate_column_diff_type_changed(self):
        """Test detecting type changes."""
        bq_columns = [
            BigQueryColumn(name="id", field_type="STRING"),
        ]
        yaml_columns = [
            DbtColumn(name="id", data_type="INT64"),
        ]

        diff = calculate_column_diff(bq_columns, yaml_columns)

        assert diff.added == set()
        assert diff.removed == set()
        assert diff.modified == set()
        assert diff.type_changed == {"id"}


class TestTableDiff:
    """Tests for table diff calculation."""

    def test_calculate_table_diff_both_exist(self):
        """Test diff when table exists in both BigQuery and YAML."""
        bq_table = BigQueryTable(
            dataset_id="test_dataset",
            table_id="test_table",
            columns=[BigQueryColumn(name="id", field_type="INT64")],
        )
        yaml_table = DbtTable(
            name="test_table",
            columns=[DbtColumn(name="id", data_type="INT64")],
        )

        diff = calculate_table_diff(bq_table, yaml_table)

        assert diff.table_name == "test_table"
        assert diff.exists_in_bigquery is True
        assert diff.exists_in_yaml is True
        assert diff.column_diff is not None
        assert diff.description_changed is False

    def test_calculate_table_diff_only_in_bigquery(self):
        """Test diff when table only exists in BigQuery."""
        bq_table = BigQueryTable(
            dataset_id="test_dataset",
            table_id="new_table",
            columns=[],
        )

        diff = calculate_table_diff(bq_table, None)

        assert diff.table_name == "new_table"
        assert diff.exists_in_bigquery is True
        assert diff.exists_in_yaml is False
        assert diff.column_diff is None

    def test_calculate_table_diff_only_in_yaml(self):
        """Test diff when table only exists in YAML."""
        yaml_table = DbtTable(name="deleted_table", columns=[])

        diff = calculate_table_diff(None, yaml_table)

        assert diff.table_name == "deleted_table"
        assert diff.exists_in_bigquery is False
        assert diff.exists_in_yaml is True
        assert diff.column_diff is None


class TestSchemaDiff:
    """Tests for schema diff calculation."""

    def test_calculate_schema_diff_complete(self):
        """Test complete schema diff calculation."""
        bq_tables = [
            BigQueryTable(
                dataset_id="test",
                table_id="table1",
                columns=[BigQueryColumn(name="id", field_type="INT64")],
            ),
            BigQueryTable(
                dataset_id="test",
                table_id="table2",
                columns=[],
            ),
        ]

        yaml_source = DbtSource(
            name="test",
            tables=[
                DbtTable(
                    name="table1", columns=[DbtColumn(name="id", data_type="INT64")]
                ),
                DbtTable(name="table3", columns=[]),
            ],
        )

        diff = calculate_schema_diff(bq_tables, yaml_source)

        assert diff.tables_added == ["table2"]
        assert diff.tables_removed == ["table3"]
        assert len(diff.table_diffs) == 3  # table1, table2, table3

    def test_calculate_schema_diff_no_yaml(self):
        """Test schema diff when YAML source doesn't exist."""
        bq_tables = [
            BigQueryTable(dataset_id="test", table_id="table1", columns=[]),
        ]

        diff = calculate_schema_diff(bq_tables, None)

        assert diff.tables_added == ["table1"]
        assert diff.tables_removed == []
        assert len(diff.table_diffs) == 1
