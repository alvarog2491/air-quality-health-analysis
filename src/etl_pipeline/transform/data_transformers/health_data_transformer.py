from typing import Dict, Tuple

import pandas as pd

from common.utils.dataframe_utils import remove_commas_and_dots

from .base_transformer import BaseTransformer


class HealthDataTransformer(BaseTransformer):
    """Transforms health data for analysis."""

    _RESP_DIS_COLUMNS_MAPPER: Dict[str, str] = {
        "Provincias": "Province",
        "Periodo": "Year",
        "Total": "Respiratory_diseases_total",
    }
    _LIFE_EXP_COLUMNS_MAPPER: Dict[str, str] = {
        "Provincias": "Province",
        "Periodo": "Year",
        "Total": "Life_expectancy_total",
    }

    def __init__(self):
        """Initialize HealthDataTransformer."""
        super().__init__(__name__)

    def transform(self, *df: pd.DataFrame) -> Tuple[pd.DataFrame, ...]:
        """
        Clean and map provinces in both DataFrames.

        Args:
            *df: respiratory_diseases_df, life_expectancy_df

        Returns:
            Tuple of cleaned DataFrames.

        Raises:
            ValueError: If input count is not 2 or any DataFrame is empty.
        """
        if len(df) != 2:
            raise ValueError("Expected exactly 2 DataFrames.")

        respiratory_df, life_expectancy_df = df

        if respiratory_df.empty or life_expectancy_df.empty:
            raise ValueError("Input DataFrame is empty")

        # Respiratory diseases
        respiratory_df.rename(
            columns=self._RESP_DIS_COLUMNS_MAPPER, inplace=True
        )
        remove_commas_and_dots(
            respiratory_df,
            columns=["Respiratory_diseases_total"],
            convert_to=float,
        )
        self._map_province_names(respiratory_df)

        # Life expectancy
        life_expectancy_df.rename(
            columns=self._LIFE_EXP_COLUMNS_MAPPER, inplace=True
        )
        self._map_province_names(life_expectancy_df)

        return respiratory_df, life_expectancy_df
