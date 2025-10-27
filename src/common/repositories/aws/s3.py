import json
from pathlib import Path
from typing import Any

from src.common.logger import Logger


class S3Repository:
    """Repository for simulating AWS S3 service by storing files locally."""

    logger = Logger(__name__)

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name

    def upload_file(self, file_path: str, data: Any) -> None:
        """
        Simulate uploading a file to S3 by creating it on disk.

        Args:
            file_path: Folder inside the bucket (subdirectory)
            data: Data to write to the file (will be serialized as JSON)
        """
        full_path = Path(self.bucket_name) / file_path
        full_path.parent.mkdir(parents=True, exist_ok=True)

        with full_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        self.logger.info(f"Successfully uploaded {file_path}")

    def get_file(self, file_path: str) -> Any:
        """Get a file from the S3 repository."""
        full_path = Path(self.bucket_name) / file_path
        if full_path.exists():
            with full_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            return data
        raise FileNotFoundError(f"File {full_path} not found")
