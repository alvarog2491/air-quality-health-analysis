import logging
from abc import ABC, abstractmethod
from typing import Iterable

import numpy as np
import pandas as pd

from etl_pipeline.utils.province_mapper import ProvinceMapper
from typing import Tuple


class BaseTransformer(ABC):
    """
    Base class for data transformers that handles common functionality
    on the transformation pipeline.
    """

    def __init__(self, name: str):
        """
        Initialize the BaseTransformer.
        """
        self.name = name
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def transform(self, *df: pd.DataFrame) -> Tuple[pd.DataFrame, ...]:
        """
        Transform the input data. Must be implemented by subclasses.

        Returns:
            Transformed DataFrame.
        """
        pass

    def _map_province_names(self, df: pd.DataFrame) -> None:
        """
        Province columns is key columns to merge all dataframes so this
        needs to be normalized in order to
        Unify province names in the dataframe.

        Parameters:
            df (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with standardized province names.
        """
        ProvinceMapper.map_province_name(df)
        self.logger.info("Province names standardized")

    def _convert_invalid_values_to_nan(
        self, df: pd.DataFrame, column: str, invalid_values: Iterable[str]
    ) -> None:
        """
        Replace invalid values in a specific column with NaN.

        Args:
            df (pd.DataFrame): The DataFrame to process.
            column (str): The name of the column to check for invalid values.
            invalid_values (list): List of invalid values to be replaced
                with NaN.

        Raises:
            ValueError: If the specified column does not exist in the
                DataFrame.
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in the DataFrame.")

        if not invalid_values:
            self.logger.warning(
                "No invalid values provided for column '%s'. Skipping "
                "replacement.",
                column,
            )
            return

        mask = df[column].isin(invalid_values)  # type: ignore
        num_replaced = mask.sum()

        if num_replaced > 0:
            df.loc[mask, column] = np.nan
            self.logger.info(
                "Replaced %d invalid values with NaN in column '%s'.",
                num_replaced,
                column,
            )
