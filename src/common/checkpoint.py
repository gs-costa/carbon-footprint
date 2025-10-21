from typing import Any

from src.repositories.aws.s3 import S3Repository


class Checkpoint(S3Repository):
    """Checkpoint class for the application."""

    CHECKPOINT_FILE = "checkpoint.json"
    CHECKPOINT_FOLDER = "checkpoints"

    def __init__(self, bucket_name: str):
        super().__init__(bucket_name)

    def save_checkpoint(self, year: int, country_code: str) -> None:
        """
        Save a checkpoint with the current year and country code.
        This allows resuming data ingestion from the last successful state.

        Args:
            year: The year being processed
            country_code: The country code being processed
        """
        checkpoint_data = {"year": year, "country_code": country_code}
        self.upload_file(file_path=f"{self.CHECKPOINT_FOLDER}/{self.CHECKPOINT_FILE}", data=checkpoint_data)

    def get_checkpoint(self) -> dict[str, Any]:
        """
        Retrieve the last saved checkpoint.

        Returns:
            Dictionary with 'year' and 'country_code' keys if checkpoint exists,
            empty dictionary otherwise
        """
        try:
            checkpoint_data = self.get_file(file_path=f"{self.CHECKPOINT_FOLDER}/{self.CHECKPOINT_FILE}")
            return checkpoint_data
        except FileNotFoundError:
            self.logger.warning("Checkpoint file not found, requesting data from API between start and end year")
            return {}
