import logging
from typing import Dict, List, Optional

import pandas as pd

# Import ValidationError from file_utils to maintain consistency
from .file_utils import ValidationError


def load_raw_dataset(
    filepath: str,
    drop_columns: Optional[List[str]] = None,
    use_cols: Optional[List[str]] = None,
    var_dtypes: Optional[Dict[str, str]] = None,
    parse_dates: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Load a raw dataset from a CSV file, with optional column selection,
    type casting, date parsing, and column dropping.

    Args:
        filepath (str): Path to the input CSV file.
        drop_columns (List[str], optional): Columns to drop after loading.
            Ignored if None.
        use_cols (List[str], optional): Subset of columns to load.
            If None, all columns are loaded.
        var_dtypes (dict, optional): Data types to apply to specific columns.
        parse_dates (List[str], optional): Columns to parse as datetime.

    Returns:
        pd.DataFrame: Loaded and preprocessed DataFrame.

    Raises:
        ValueError: If any column appears in both use_cols and drop_columns.
        ValueError: If the resulting DataFrame is empty.
    """
    if use_cols and drop_columns:
        overlap = set(use_cols) & set(drop_columns)
        if overlap:
            raise ValueError(
                f"Conflict detected: Columns {overlap} are present in both "
                f"'use_cols' and 'drop_columns'."
            )

    df = pd.read_csv(
        filepath,
        usecols=use_cols,
        dtype=var_dtypes,
        parse_dates=parse_dates
    )  # type: ignore

    if drop_columns:
        df = df.drop(columns=drop_columns, errors="ignore")

    df.reset_index(drop=True, inplace=True)

    if df.empty:
        raise ValueError("The loaded dataset is empty after applying filters.")

    return df


def convert_to_dataframe_with_dtypes(
    data: Dict[str, List],
    var_dtypes: Dict[str, str],
) -> pd.DataFrame:
    """
    Convert a dictionary of lists to a DataFrame with specified data types.

    Args:
        data (Dict[str, List]): Dictionary where keys are column names
            and values are lists of column values.
        var_dtypes (Dict[str, str]): Dictionary mapping column names
            to their desired data types.

    Returns:
        pd.DataFrame: DataFrame with the specified data types applied
            to each column.
    """
    df = pd.DataFrame(data)
    for column, dtype in var_dtypes.items():
        if column in df.columns:
            df[column] = df[column].astype(dtype)
    return df


def validate_no_missing_values(df: pd.DataFrame) -> None:
    """
    Raise an error if the DataFrame contains any missing values.

    Args:
        df (pd.DataFrame): DataFrame to validate.

    Raises:
        ValueError: If missing values are found.
    """
    if df.isnull().values.any():
        raise ValueError(
            "Data contains missing values. Please handle them "
            "before proceeding."
        )


def remove_commas_and_dots(
    df: pd.DataFrame, columns: List[str], convert_to: type
) -> None:
    """
    Remove commas and dots from string values in the given columns,
    and convert the result to a specified type.

    Args:
        df (pd.DataFrame): DataFrame containing the target columns.
        columns (List[str]): Columns to clean and convert.
        convert_to (type): Target data type (e.g., int, float).

    Returns:
        None. The DataFrame is modified in place.
    """
    for column in columns:
        df[column] = (
            df[column]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .str.replace(".", "", regex=False)
            .astype(convert_to)
        )
        logging.info(
            f"Removed ',' and '.' from '{column}' column and converted to "
            f"{convert_to.__name__}"
        )


def remove_dots(
    df: pd.DataFrame, columns: List[str], convert_to: type
) -> None:
    """
    Remove dots from string values in the specified columns and convert
    to a given type.

    Args:
        df (pd.DataFrame): DataFrame containing the target columns.
        columns (List[str]): Columns to clean and convert.
        convert_to (type): Target data type (e.g., int, float).

    Returns:
        None. The DataFrame is modified in place.
    """
    for column in columns:
        df[column] = (
            df[column]
            .astype(str)
            .str.replace(".", "", regex=False)
            .astype(convert_to)
        )
        logging.info(
            f"Removed '.' from '{column}' column and converted to "
            f"{convert_to.__name__}"
        )


def log_null_values(df: pd.DataFrame) -> None:
    """
    Log the count of null values per column, if any.

    Args:
        df (pd.DataFrame): DataFrame to check.
    """
    null_counts = df.isnull().sum()
    if null_counts.any():
        logging.warning(
            f"Null values detected in columns: "
            f"{null_counts[null_counts > 0].to_dict()}"  # type: ignore
        )


def log_duplicated_rows(df: pd.DataFrame) -> None:
    """
    Log the number of duplicated rows in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame to check.
    """
    duplicated_count = df.duplicated().sum()
    if duplicated_count:
        logging.warning(f"Duplicated rows found: {duplicated_count}")


def log_info(df: pd.DataFrame) -> None:
    """
    Log the structure and metadata of the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame to describe.
    """
    logging.info("DataFrame info:")
    logging.info(df.info())


def log_empty_rows(df: pd.DataFrame) -> None:
    """
    Log the number of rows with more than 70% missing values.

    Args:
        df (pd.DataFrame): DataFrame to evaluate.
    """
    empty_rows = df.isnull().mean(axis=1) > 0.7
    if empty_rows.any():
        logging.warning(
            "%d rows contain more than 70%% missing values.", empty_rows.sum()
        )


def log_memory_usage(df: pd.DataFrame) -> None:
    """
    Log the approximate memory usage of the DataFrame in MB.

    Args:
        df (pd.DataFrame): DataFrame to analyze.
    """
    memory_usage = df.memory_usage(deep=True).sum() / 1024**2
    logging.info(f"(~{memory_usage:.1f} MB memory usage)")


def validate_dataframe_columns(
    df: pd.DataFrame, required_columns: List[str], df_name: str = "DataFrame"
) -> None:
    """
    Validate that a DataFrame contains required columns.

    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        df_name: Name of the DataFrame for error messages

    Raises:
        ValidationError: If required columns are missing
    """
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValidationError(
            f"{df_name} missing required columns: {sorted(missing_columns)}. "
            f"Available columns: {sorted(df.columns)}"
        )


def validate_dataframe_not_empty(
    df: pd.DataFrame, df_name: str = "DataFrame"
) -> None:
    """
    Validate that a DataFrame is not empty.

    Args:
        df: DataFrame to validate
        df_name: Name of the DataFrame for error messages

    Raises:
        ValidationError: If DataFrame is empty
    """
    if df.empty:
        raise ValidationError(f"{df_name} is empty")
    if len(df) == 0:
        raise ValidationError(f"{df_name} has no rows")


def validate_data_shapes_match(X: pd.DataFrame, y: pd.Series) -> None:
    """
    Validate that features and target have matching number of samples.

    Args:
        X: Features DataFrame
        y: Target Series

    Raises:
        ValidationError: If shapes don't match
    """
    if len(X) != len(y):
        raise ValidationError(
            f"Features and target have different number of samples: "
            f"X has {len(X)} samples, y has {len(y)} samples"
        )


def validate_target_column(df: pd.DataFrame, target_column: str) -> None:
    """
    Validate that target column exists and has valid values.

    Args:
        df: DataFrame containing the target column
        target_column: Name of the target column

    Raises:
        ValidationError: If target column is invalid
    """
    validate_dataframe_columns(df, [target_column], "Dataset")

    target_series = df[target_column]
    if target_series.isna().all():
        raise ValidationError(
            f"Target column '{target_column}' contains only null values"
        )

    null_percentage = target_series.isna().sum() / len(target_series) * 100
    if null_percentage > 50:
        raise ValidationError(
            f"Target column '{target_column}' has {null_percentage:.1f}% "
            f"null values (too high)"
        )
