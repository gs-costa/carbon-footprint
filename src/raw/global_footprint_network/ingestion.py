from src.common.environment import Environment
from src.common.logger import Logger
from src.common.repositories.aws.s3 import S3Repository


class GlobalFootprintNetworkIngestion:
    """ETL for the Global Footprint Network API."""

    logger = Logger(__name__)
    environment = Environment()

    def __init__(self):
        self.s3_repository = S3Repository(bucket_name=self.environment.RAW_PATH)

    def load(self, file_path: str, data: list[dict]) -> None:
        """Load data into the S3 repository (Simulate Lambda 2)."""
        self.s3_repository.upload_file(file_path=file_path, data=data)
