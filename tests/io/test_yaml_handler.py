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

    def test_file_update_preserves_existing_keys(self):
        """Test that updating a file preserves existing keys and structure."""
        # Create initial file with existing structure
        initial_source_file = DbtSourceFile(
            version=2,
            sources=[
                DbtSource(
                    name="existing_source",
                    database="project_id",
                    schema="dataset_id",
                    description="Original source description",
                    meta={"team": "analytics", "priority": "high"},
                    tables=[
                        DbtTable(
                            name="existing_table",
                            description="Original table description",
                            meta={"updated_at": "2023-01-01"},
                            columns=[
                                DbtColumn(
                                    name="id",
                                    data_type="INT64",
                                    description="Primary key",
                                    meta={"pii": False},
                                ),
                                DbtColumn(
                                    name="name",
                                    data_type="STRING",
                                    description="User name",
                                    meta={"pii": True},
                                ),
                            ],
                        ),
                        DbtTable(
                            name="preserved_table",
                            description="This table should be preserved",
                            columns=[
                                DbtColumn(name="preserved_col", data_type="STRING"),
                            ],
                        ),
                    ],
                ),
            ],
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            temp_path = f.name

        try:
            # Write initial file
            self.yaml_handler.write_source_file(temp_path, initial_source_file)

            # Read and verify initial structure
            read_file = self.yaml_handler.read_source_file(temp_path)
            assert read_file is not None
            assert read_file.sources[0].meta == {"team": "analytics", "priority": "high"}
            assert read_file.sources[0].tables[0].meta == {"updated_at": "2023-01-01"}
            assert read_file.sources[0].tables[0].columns[0].meta == {"pii": False}

            # Create updated structure simulating merge with remote changes
            updated_source_file = DbtSourceFile(
                version=2,
                sources=[
                    DbtSource(
                        name="existing_source",
                        database="project_id",
                        schema="dataset_id",
                        description="Original source description",  # Preserved
                        meta={"team": "analytics", "priority": "high"},  # Preserved
                        tables=[
                            DbtTable(
                                name="existing_table",
                                description="Original table description",  # Preserved
                                meta={"updated_at": "2023-01-01"},  # Preserved
                                columns=[
                                    DbtColumn(
                                        name="id",
                                        data_type="INT64",
                                        description="Primary key",  # Preserved
                                        meta={"pii": False},  # Preserved
                                    ),
                                    DbtColumn(
                                        name="name",
                                        data_type="STRING",
                                        description="User name",  # Preserved
                                        meta={"pii": True},  # Preserved
                                    ),
                                    DbtColumn(
                                        name="new_remote_column",
                                        data_type="TIMESTAMP",
                                        description="Added from remote",
                                    ),
                                ],
                            ),
                            DbtTable(
                                name="preserved_table",
                                description="This table should be preserved",  # Preserved
                                columns=[
                                    DbtColumn(name="preserved_col", data_type="STRING"),
                                ],
                            ),
                        ],
                    ),
                ],
            )

            # Update the file
            self.yaml_handler.write_source_file(temp_path, updated_source_file)

            # Read updated file and verify preservation
            final_file = self.yaml_handler.read_source_file(temp_path)
            assert final_file is not None

            # Verify source-level metadata preserved
            source = final_file.sources[0]
            assert source.name == "existing_source"
            assert source.description == "Original source description"
            assert source.meta == {"team": "analytics", "priority": "high"}

            # Verify table-level metadata preserved
            existing_table = next(t for t in source.tables if t.name == "existing_table")
            assert existing_table.description == "Original table description"
            assert existing_table.meta == {"updated_at": "2023-01-01"}

            # Verify preserved table still exists
            preserved_table = next(t for t in source.tables if t.name == "preserved_table")
            assert preserved_table.description == "This table should be preserved"

            # Verify column-level metadata preserved
            id_col = next(c for c in existing_table.columns if c.name == "id")
            assert id_col.description == "Primary key"
            assert id_col.meta == {"pii": False}

            name_col = next(c for c in existing_table.columns if c.name == "name")
            assert name_col.description == "User name"
            assert name_col.meta == {"pii": True}

            # Verify new column was added
            new_col = next(c for c in existing_table.columns if c.name == "new_remote_column")
            assert new_col.data_type == "TIMESTAMP"
            assert new_col.description == "Added from remote"

        finally:
            Path(temp_path).unlink()

    def test_table_column_addition_scenarios(self):
        """Test adding tables and columns that exist in remote but not locally."""
        # Initial YAML with minimal structure
        initial_source_file = DbtSourceFile(
            version=2,
            sources=[
                DbtSource(
                    name="test_source",
                    schema="test_dataset",
                    tables=[
                        DbtTable(
                            name="existing_table",
                            columns=[
                                DbtColumn(name="id", data_type="INT64"),
                            ],
                        ),
                    ],
                ),
            ],
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            temp_path = f.name

        try:
            # Write initial file
            self.yaml_handler.write_source_file(temp_path, initial_source_file)

            # Simulate remote changes: new table and new columns
            updated_source_file = DbtSourceFile(
                version=2,
                sources=[
                    DbtSource(
                        name="test_source",
                        schema="test_dataset",
                        tables=[
                            DbtTable(
                                name="existing_table",
                                columns=[
                                    DbtColumn(name="id", data_type="INT64"),
                                    DbtColumn(
                                        name="new_column_1",
                                        data_type="STRING",
                                        description="Added from BigQuery",
                                    ),
                                    DbtColumn(
                                        name="new_column_2",
                                        data_type="TIMESTAMP",
                                        description="Another new column",
                                    ),
                                ],
                            ),
                            DbtTable(
                                name="new_remote_table",
                                description="Table discovered in BigQuery",
                                columns=[
                                    DbtColumn(
                                        name="remote_id",
                                        data_type="INT64",
                                        description="Remote table primary key",
                                    ),
                                    DbtColumn(
                                        name="remote_name",
                                        data_type="STRING",
                                        description="Remote table name field",
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            )

            # Update the file
            self.yaml_handler.write_source_file(temp_path, updated_source_file)

            # Verify additions
            final_file = self.yaml_handler.read_source_file(temp_path)
            assert final_file is not None

            source = final_file.sources[0]
            assert len(source.tables) == 2

            # Verify existing table with new columns
            existing_table = next(t for t in source.tables if t.name == "existing_table")
            assert len(existing_table.columns) == 3
            assert existing_table.columns[0].name == "id"
            assert existing_table.columns[1].name == "new_column_1"
            assert existing_table.columns[2].name == "new_column_2"

            # Verify new table
            new_table = next(t for t in source.tables if t.name == "new_remote_table")
            assert new_table.description == "Table discovered in BigQuery"
            assert len(new_table.columns) == 2
            assert new_table.columns[0].name == "remote_id"
            assert new_table.columns[1].name == "remote_name"

        finally:
            Path(temp_path).unlink()

    def test_column_deletion_when_not_in_remote(self):
        """Test deletion of columns that exist in YAML but not in remote source."""
        # Initial YAML with columns that will be "deleted" from remote
        initial_source_file = DbtSourceFile(
            version=2,
            sources=[
                DbtSource(
                    name="test_source",
                    schema="test_dataset",
                    tables=[
                        DbtTable(
                            name="test_table",
                            columns=[
                                DbtColumn(
                                    name="id",
                                    data_type="INT64",
                                    description="Primary key",
                                ),
                                DbtColumn(
                                    name="name",
                                    data_type="STRING",
                                    description="Name field",
                                ),
                                DbtColumn(
                                    name="deleted_column",
                                    data_type="STRING",
                                    description="This will be removed",
                                ),
                                DbtColumn(
                                    name="another_deleted",
                                    data_type="INT64",
                                    description="This will also be removed",
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            temp_path = f.name

        try:
            # Write initial file
            self.yaml_handler.write_source_file(temp_path, initial_source_file)

            # Simulate remote having fewer columns (some deleted)
            updated_source_file = DbtSourceFile(
                version=2,
                sources=[
                    DbtSource(
                        name="test_source",
                        schema="test_dataset",
                        tables=[
                            DbtTable(
                                name="test_table",
                                columns=[
                                    DbtColumn(
                                        name="id",
                                        data_type="INT64",
                                        description="Primary key",
                                    ),
                                    DbtColumn(
                                        name="name",
                                        data_type="STRING",
                                        description="Name field",
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            )

            # Update the file (simulating removal of deleted columns)
            self.yaml_handler.write_source_file(temp_path, updated_source_file)

            # Verify columns were removed
            final_file = self.yaml_handler.read_source_file(temp_path)
            assert final_file is not None

            table = final_file.sources[0].tables[0]
            assert len(table.columns) == 2
            
            column_names = [col.name for col in table.columns]
            assert "id" in column_names
            assert "name" in column_names
            assert "deleted_column" not in column_names
            assert "another_deleted" not in column_names

            # Verify remaining columns preserved their metadata
            id_col = next(c for c in table.columns if c.name == "id")
            assert id_col.description == "Primary key"

        finally:
            Path(temp_path).unlink()

    def test_table_preservation_when_not_in_remote(self):
        """Test that tables not in remote are preserved based on merge strategy."""
        # Initial YAML with tables, some of which don't exist in remote
        initial_source_file = DbtSourceFile(
            version=2,
            sources=[
                DbtSource(
                    name="test_source",
                    schema="test_dataset",
                    tables=[
                        DbtTable(
                            name="remote_table",
                            description="Exists in both YAML and remote",
                            columns=[
                                DbtColumn(name="id", data_type="INT64"),
                            ],
                        ),
                        DbtTable(
                            name="yaml_only_table_1",
                            description="Only exists in YAML - should be preserved",
                            columns=[
                                DbtColumn(
                                    name="yaml_col",
                                    data_type="STRING",
                                    description="YAML-only column",
                                ),
                            ],
                        ),
                        DbtTable(
                            name="yaml_only_table_2",
                            description="Another YAML-only table",
                            columns=[],
                        ),
                    ],
                ),
            ],
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            temp_path = f.name

        try:
            # Write initial file
            self.yaml_handler.write_source_file(temp_path, initial_source_file)

            # Simulate scenario where only some tables exist in remote
            # (preserving YAML-only tables as per requirements)
            updated_source_file = DbtSourceFile(
                version=2,
                sources=[
                    DbtSource(
                        name="test_source",
                        schema="test_dataset",
                        tables=[
                            DbtTable(
                                name="remote_table",
                                description="Exists in both YAML and remote",
                                columns=[
                                    DbtColumn(name="id", data_type="INT64"),
                                    DbtColumn(
                                        name="new_remote_col",
                                        data_type="STRING",
                                        description="Added from remote",
                                    ),
                                ],
                            ),
                            DbtTable(
                                name="yaml_only_table_1",
                                description="Only exists in YAML - should be preserved",
                                columns=[
                                    DbtColumn(
                                        name="yaml_col",
                                        data_type="STRING",
                                        description="YAML-only column",
                                    ),
                                ],
                            ),
                            DbtTable(
                                name="yaml_only_table_2",
                                description="Another YAML-only table",
                                columns=[],
                            ),
                            DbtTable(
                                name="new_remote_table",
                                description="New table from remote",
                                columns=[
                                    DbtColumn(
                                        name="remote_id",
                                        data_type="INT64",
                                        description="Remote table ID",
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            )

            # Update the file
            self.yaml_handler.write_source_file(temp_path, updated_source_file)

            # Verify table preservation and updates
            final_file = self.yaml_handler.read_source_file(temp_path)
            assert final_file is not None

            source = final_file.sources[0]
            assert len(source.tables) == 4

            table_names = [table.name for table in source.tables]
            assert "remote_table" in table_names
            assert "yaml_only_table_1" in table_names
            assert "yaml_only_table_2" in table_names
            assert "new_remote_table" in table_names

            # Verify YAML-only tables preserved their structure
            yaml_table_1 = next(t for t in source.tables if t.name == "yaml_only_table_1")
            assert yaml_table_1.description == "Only exists in YAML - should be preserved"
            assert len(yaml_table_1.columns) == 1
            assert yaml_table_1.columns[0].name == "yaml_col"
            assert yaml_table_1.columns[0].description == "YAML-only column"

            yaml_table_2 = next(t for t in source.tables if t.name == "yaml_only_table_2")
            assert yaml_table_2.description == "Another YAML-only table"
            assert len(yaml_table_2.columns) == 0

            # Verify remote table was updated
            remote_table = next(t for t in source.tables if t.name == "remote_table")
            assert len(remote_table.columns) == 2
            assert remote_table.columns[1].name == "new_remote_col"

            # Verify new remote table was added
            new_remote_table = next(t for t in source.tables if t.name == "new_remote_table")
            assert new_remote_table.description == "New table from remote"

        finally:
            Path(temp_path).unlink()

    def test_complex_update_scenario_preserving_structure(self):
        """Test complex update scenario that combines all requirements."""
        # Create a complex initial structure
        initial_source_file = DbtSourceFile(
            version=2,
            sources=[
                DbtSource(
                    name="complex_source",
                    database="project",
                    schema="dataset",
                    description="Complex source for testing",
                    meta={"team": "data", "environment": "prod"},
                    tables=[
                        DbtTable(
                            name="users",
                            description="User table with important metadata",
                            meta={"contains_pii": True, "retention_days": 365},
                            columns=[
                                DbtColumn(
                                    name="id",
                                    data_type="INT64",
                                    description="User ID",
                                    meta={"primary_key": True},
                                ),
                                DbtColumn(
                                    name="email",
                                    data_type="STRING",
                                    description="User email address",
                                    meta={"pii": True},
                                ),
                                DbtColumn(
                                    name="old_column",
                                    data_type="STRING",
                                    description="This will be removed from remote",
                                ),
                            ],
                        ),
                        DbtTable(
                            name="yaml_only_table",
                            description="This table exists only in YAML",
                            meta={"manual_table": True},
                            columns=[
                                DbtColumn(
                                    name="manual_id",
                                    data_type="STRING",
                                    description="Manually maintained ID",
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            temp_path = f.name

        try:
            # Write initial file
            self.yaml_handler.write_source_file(temp_path, initial_source_file)

            # Create updated structure simulating complex remote changes
            updated_source_file = DbtSourceFile(
                version=2,
                sources=[
                    DbtSource(
                        name="complex_source",
                        database="project",
                        schema="dataset",
                        description="Complex source for testing",  # Preserved
                        meta={"team": "data", "environment": "prod"},  # Preserved
                        tables=[
                            DbtTable(
                                name="users",
                                description="User table with important metadata",  # Preserved
                                meta={"contains_pii": True, "retention_days": 365},  # Preserved
                                columns=[
                                    DbtColumn(
                                        name="id",
                                        data_type="INT64",
                                        description="User ID",  # Preserved
                                        meta={"primary_key": True},  # Preserved
                                    ),
                                    DbtColumn(
                                        name="email",
                                        data_type="STRING",
                                        description="User email address",  # Preserved
                                        meta={"pii": True},  # Preserved
                                    ),
                                    # old_column removed (not in remote)
                                    DbtColumn(
                                        name="created_at",
                                        data_type="TIMESTAMP",
                                        description="User creation timestamp",  # New from remote
                                    ),
                                    DbtColumn(
                                        name="updated_at",
                                        data_type="TIMESTAMP",
                                        description="User update timestamp",  # New from remote
                                    ),
                                ],
                            ),
                            DbtTable(
                                name="yaml_only_table",
                                description="This table exists only in YAML",  # Preserved
                                meta={"manual_table": True},  # Preserved
                                columns=[
                                    DbtColumn(
                                        name="manual_id",
                                        data_type="STRING",
                                        description="Manually maintained ID",  # Preserved
                                    ),
                                ],
                            ),
                            DbtTable(
                                name="orders",
                                description="New orders table from remote",  # New table
                                columns=[
                                    DbtColumn(
                                        name="order_id",
                                        data_type="INT64",
                                        description="Order identifier",
                                    ),
                                    DbtColumn(
                                        name="user_id",
                                        data_type="INT64",
                                        description="User who placed order",
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            )

            # Update the file
            self.yaml_handler.write_source_file(temp_path, updated_source_file)

            # Comprehensive verification
            final_file = self.yaml_handler.read_source_file(temp_path)
            assert final_file is not None

            source = final_file.sources[0]
            
            # Verify source-level preservation
            assert source.name == "complex_source"
            assert source.description == "Complex source for testing"
            assert source.meta == {"team": "data", "environment": "prod"}
            
            # Verify table count and names
            assert len(source.tables) == 3
            table_names = [t.name for t in source.tables]
            assert "users" in table_names
            assert "yaml_only_table" in table_names
            assert "orders" in table_names

            # Verify users table updates
            users_table = next(t for t in source.tables if t.name == "users")
            assert users_table.description == "User table with important metadata"
            assert users_table.meta == {"contains_pii": True, "retention_days": 365}
            assert len(users_table.columns) == 4  # id, email, created_at, updated_at

            # Verify preserved columns
            id_col = next(c for c in users_table.columns if c.name == "id")
            assert id_col.description == "User ID"
            assert id_col.meta == {"primary_key": True}

            email_col = next(c for c in users_table.columns if c.name == "email")
            assert email_col.description == "User email address"
            assert email_col.meta == {"pii": True}

            # Verify new columns
            created_col = next(c for c in users_table.columns if c.name == "created_at")
            assert created_col.description == "User creation timestamp"

            # Verify old_column was removed
            old_cols = [c for c in users_table.columns if c.name == "old_column"]
            assert len(old_cols) == 0

            # Verify YAML-only table preservation
            yaml_table = next(t for t in source.tables if t.name == "yaml_only_table")
            assert yaml_table.description == "This table exists only in YAML"
            assert yaml_table.meta == {"manual_table": True}
            assert len(yaml_table.columns) == 1
            assert yaml_table.columns[0].name == "manual_id"

            # Verify new table addition
            orders_table = next(t for t in source.tables if t.name == "orders")
            assert orders_table.description == "New orders table from remote"
            assert len(orders_table.columns) == 2

        finally:
            Path(temp_path).unlink()
