import pandas as pd


def enforce_types(df: pd.DataFrame, columns: dict) -> pd.DataFrame:
    """Enforce types on a DataFrame."""

    for column, data_type in columns.items():
        if column in df.columns:
            df[column] = df[column].astype(data_type)
    return df
