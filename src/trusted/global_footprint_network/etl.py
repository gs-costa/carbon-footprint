import os

import pandas as pd

from src.common.environment import Environment
from src.common.etl.functions import enforce_types, range_years
from src.common.etl.mixins.file_system import FileSystemETLMixin
from src.common.logger import Logger
from src.repositories.duckdb import DuckDBRepository
from src.trusted.global_footprint_network.schema import GlobalFootprintNetworkSchema


class GlobalFootprintNetworkTrusted(FileSystemETLMixin):
    """Curated data from the Global Footprint Network API."""

    logger = Logger(__name__)
    FILE_FORMAT = "json"
    TABLE_NAME = "carbon_footprint"

    def __init__(self):
        self.environment = Environment()
        self.schema = GlobalFootprintNetworkSchema()
        self.duckdb_repository = DuckDBRepository(database_path=self.environment.DATABASE_PATH)

    def read(self, year: int) -> pd.DataFrame:
        file_path = os.path.join(self.environment.RAW_PATH, "data", str(year))
        try:
            return self.read_json_with_pandas(file_path=file_path)
        except FileNotFoundError:
            self.logger.warning(f"File not found: {file_path}")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error reading file: {file_path}")
            raise e

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        renamed_df = df.rename(columns=self.schema.rename_columns)
        enforced_df = enforce_types(df=renamed_df, columns=self.schema.enforce_types)
        selected_df = enforced_df[self.schema.select_columns]
        return selected_df

    def load(self, df: pd.DataFrame) -> None:
        self.duckdb_repository.upsert_data_from_pandas(
            df=df,
            table_name=self.TABLE_NAME,
            schema_duckdb=self.schema.schema_duckdb,
            key_columns=self.schema.merge_key_columns,
        )

    def execute(self) -> None:
        years = range_years(self.environment.START_YEAR, self.environment.END_YEAR)
        try:
            for year in years:
                data = self.read(year=year)
                if data.empty:
                    self.logger.warning(f"DataFrame is empty for year {year}")
                    continue
                transformed_data = self.transform(df=data)
                self.load(df=transformed_data)
        finally:
            self.duckdb_repository.close_connection()


if __name__ == "__main__":
    client = GlobalFootprintNetworkTrusted()
    client.execute()
