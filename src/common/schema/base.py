from dataclasses import dataclass

from src.common.schema.conversion_types_python_duckdb import mapping_types_pandas_duckdb


@dataclass
class Column:
    """Class to represent a Column in a schema"""

    name: str
    column_type: str
    rename: str | None = None
    merge_key: bool = False


class BaseSchema:
    columns: list[Column]

    @property
    def rename_columns(self) -> dict[str, str]:
        return {column.rename: column.name for column in self.columns if column.rename}

    @property
    def enforce_types(self) -> dict[str, str]:
        return {column.name: column.column_type for column in self.columns}

    @property
    def schema_duckdb(self) -> dict[str, str]:
        return [f"{column.name} {mapping_types_pandas_duckdb[column.column_type]}" for column in self.columns]

    @property
    def select_columns(self) -> list[str]:
        return [column.name for column in self.columns]

    @property
    def merge_key_columns(self) -> list[str]:
        return [column.name for column in self.columns if column.merge_key]
