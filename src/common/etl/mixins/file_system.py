import glob
import os

import pandas as pd


class FileSystemETLMixin:
    """Mixin for file system operations."""

    def file_exists(self, file_path: str) -> bool:
        """Check if a file exists."""
        return os.path.exists(file_path)

    def list_json_files(self, folder_path: str) -> list[str]:
        """List all JSON files in a folder."""
        pattern = os.path.join(folder_path, "*.json")
        return glob.glob(pattern)

    def read_json_with_pandas(self, file_path: str) -> pd.DataFrame:
        """Read JSON files in a folder into a Pandas DataFrame."""

        json_files = self.list_json_files(file_path)
        if not json_files:
            raise FileNotFoundError(f"File not found: {file_path}")

        df_list = []
        for json_file in json_files:
            df = pd.read_json(json_file)
            df_list.append(df)

        return pd.concat(df_list)
