import pandas as pd
from duckdb import DuckDBPyConnection, connect

from src.common.logger import Logger


class DuckDBRepository:
    """Repository for DuckDB database."""

    logger = Logger(__name__)

    def __init__(self, database_path: str):
        self.database_path = database_path
        self.connection: DuckDBPyConnection = connect(self.database_path)

    def create_table(self, table_name: str, schema: list[str]) -> None:
        """Create a table in the database.
        Args:
            table_name: Name of the table to create
            schema: List of columns (formatted as "column_name column_type") to create
        """
        self.connection.sql(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(schema)})")

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        try:
            result = self.connection.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
            ).fetchone()
            if result is None:
                return False
            return result[0] > 0
        except Exception as e:
            self.logger.error(f"Error checking if table {table_name} exists: {str(e)}")
            return False

    def close_connection(self) -> None:
        """Close the connection to the database."""
        self.connection.close()
        self.logger.info("Connection to DuckDB closed")

    def insert_data_from_pandas(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema_duckdb: list[str] | None = None,
    ) -> None:
        """Insert data from a pandas DataFrame into a DuckDB table."""

        try:
            table_exists = self.table_exists(table_name)
            if not table_exists:
                if schema_duckdb is None:
                    raise ValueError(f"Table {table_name} doesn't exist and no schema provided")
                self.create_table(table_name=table_name, schema=schema_duckdb)

            self.connection.execute(f"INSERT INTO {table_name} BY NAME SELECT * FROM df")
            self.logger.info(f"Inserted {len(df)} rows into table {table_name}")

        except Exception as e:
            self.logger.error(f"Failed to insert data into table {table_name}: {str(e)}")
            raise

    def upsert_data_from_pandas(
        self,
        df: pd.DataFrame,
        table_name: str,
        key_columns: list[str],
        schema_duckdb: list[str] | None = None,
    ) -> None:
        """Insert or update data from a pandas DataFrame into a DuckDB table."""
        try:
            if df.empty:
                self.logger.warning(f"DataFrame is empty, skipping upsert for table {table_name}")
                return

            missing_keys = [col for col in key_columns if col not in df.columns]
            if missing_keys:
                raise ValueError(f"Key columns {missing_keys} not found in DataFrame")

            table_exists = self.table_exists(table_name)

            if not table_exists:
                self.insert_data_from_pandas(df=df, table_name=table_name, schema_duckdb=schema_duckdb)
                return

            self._perform_upsert_merge(
                df=df, table_name=table_name, key_columns=key_columns, schema_duckdb=schema_duckdb
            )

        except Exception as e:
            self.logger.error(f"Failed to upsert data into table {table_name}: {str(e)}")
            raise

    def _perform_upsert_merge(
        self, df: pd.DataFrame, table_name: str, key_columns: list[str], schema_duckdb: list[str] | None = None
    ) -> None:
        """Perform upsert using DuckDB's MERGE statement."""
        try:
            temp_table = f"{table_name}_temp"

            self.connection.execute(f"CREATE TEMP TABLE {temp_table} AS SELECT * FROM df")

            key_conditions = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])

            all_columns = df.columns.tolist()
            update_columns = [col for col in all_columns if col not in key_columns]

            if update_columns:
                update_set = ", ".join([f"{col} = source.{col}" for col in update_columns])
                insert_columns = ", ".join(all_columns)
                insert_values = ", ".join([f"source.{col}" for col in all_columns])
                merge_sql = f"""
                MERGE INTO {table_name} AS target
                USING {temp_table} AS source
                ON {key_conditions}
                WHEN MATCHED THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
                """

            self.connection.execute(merge_sql)

            self.logger.info(f"Successfully upserted {len(df)} rows into table {table_name}")

        except Exception as e:
            self.logger.error(f"Error during merge operation: {str(e)}")
            raise
        finally:
            self.connection.execute(f"DROP TABLE IF EXISTS {temp_table}")
