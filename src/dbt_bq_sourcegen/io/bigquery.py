"""BigQuery client for fetching schema information."""

import logging
from typing import Optional

from google.cloud import bigquery

from ..types.bigquery import BigQueryColumn, BigQueryDataset, BigQueryTable

logger = logging.getLogger(__name__)


class BigQueryClient:
    """Client for interacting with BigQuery."""

    def __init__(self, project_id: str):
        """Initialize BigQuery client.

        Args:
            project_id: Google Cloud project ID.
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def get_dataset_schema(
        self,
        dataset_id: str,
        table_pattern: Optional[str] = None,
        exclude_pattern: Optional[str] = None,
    ) -> BigQueryDataset:
        """Get complete schema information for a dataset.

        Args:
            dataset_id: BigQuery dataset ID.
            table_pattern: Optional wildcard pattern to filter tables (e.g., 'stg_*').
            exclude_pattern: Optional pattern to exclude tables.

        Returns:
            BigQueryDataset with all tables and columns.
        """
        tables = []

        try:
            dataset_ref = self.client.dataset(dataset_id)
            table_list = self.client.list_tables(dataset_ref)

            for table_item in table_list:
                table_id = table_item.table_id

                # Apply filters
                if table_pattern and not self._matches_pattern(table_id, table_pattern):
                    continue
                if exclude_pattern and exclude_pattern in table_id:
                    continue

                try:
                    table = self.get_table_schema(dataset_id, table_id)
                    tables.append(table)
                except Exception as e:
                    logger.error(f"Failed to get schema for table {table_id}: {e}")

        except Exception as e:
            logger.error(f"Failed to list tables in dataset {dataset_id}: {e}")

        return BigQueryDataset(
            project_id=self.project_id,
            dataset_id=dataset_id,
            tables=tables,
        )

    def get_table_schema(self, dataset_id: str, table_id: str) -> BigQueryTable:
        """Get schema information for a specific table.

        Args:
            dataset_id: BigQuery dataset ID.
            table_id: BigQuery table ID.

        Returns:
            BigQueryTable with column information.
        """
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = self.client.get_table(table_ref)

        columns = [
            BigQueryColumn(
                name=field.name,
                field_type=field.field_type,
                mode=field.mode,
                description=field.description,
            )
            for field in table.schema
        ]

        return BigQueryTable(
            dataset_id=dataset_id,
            table_id=table_id,
            description=table.description,
            columns=columns,
        )

    def _matches_pattern(self, table_id: str, pattern: str) -> bool:
        """Check if table ID matches a wildcard pattern.

        Args:
            table_id: Table ID to check.
            pattern: Wildcard pattern (e.g., 'stg_*').

        Returns:
            True if matches, False otherwise.
        """
        import fnmatch

        return fnmatch.fnmatch(table_id, pattern)
