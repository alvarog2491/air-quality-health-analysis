from typing import Tuple

import pandas as pd

from common.utils.dataframe_utils import remove_dots

from .base_transformer import BaseTransformer


class SocioeconomicDataTransformer(BaseTransformer):
    """
    Transformer for socioeconomic data, specifically GDP and population
    by province.
    """

    _GDP_COLUMNS_MAPPER = {
        "value": "pib",
        "Provincia": "Province",
        "anio": "Year",
    }

    _POPULATION_COLUMNS_MAPPER = {
        "Total": "Population",
        "Periodo": "Year",
        "Provincias": "Province",
    }

    def __init__(self):
        super().__init__(__name__)

    def transform(self, *df: pd.DataFrame) -> Tuple[pd.DataFrame, ...]:
        """
        Clean and standardize the loaded DataFrames.

        Args:
            *df: gdp_df, population_df

        Returns:
            Tuple of cleaned DataFrames.

        Raises:
            ValueError: If input count is not 2 or any DataFrame is empty.
        """
        if len(df) != 2:
            raise ValueError("Expected exactly 2 DataFrames.")

        gdp_df, population_df = df

        if gdp_df.empty or population_df.empty:
            raise ValueError("Input DataFrame is empty")

        # Transform GDP and population DataFrames
        gdp_df = self._transform_gdp_columns(gdp_df)
        population_df = self._transform_population_columns(population_df)

        return gdp_df, population_df

    def _transform_gdp_columns(self, gdp_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform GDP columns from wide to long format.

        Args:
            gdp_df: GDP DataFrame in wide format

        Returns:
            GDP DataFrame in long format with standardized columns.
        """
        self.logger.info("Transforming GDP DataFrame from wide to long format")

        gdp_df = gdp_df.melt(  # type: ignore
            id_vars=["Provincia"], var_name="anio"
        )
        gdp_df.rename(columns=self._GDP_COLUMNS_MAPPER, inplace=True)
        gdp_df["Year"] = pd.to_datetime(gdp_df["Year"], format="%Y")
        gdp_df["pib"] = gdp_df["pib"].astype(float)
        self._map_province_names(gdp_df)

        return gdp_df

    def _transform_population_columns(
        self, province_population_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Clean population size DataFrame.

        Args:
            province_population_df: Population DataFrame to clean.

        Returns:
            Cleaned population DataFrame with standardized columns.
        """
        self.logger.info("Transforming province population DataFrame")

        province_population_df.rename(
            columns=self._POPULATION_COLUMNS_MAPPER, inplace=True
        )
        remove_dots(
            province_population_df, columns=["Population"], convert_to=int
        )
        self._map_province_names(province_population_df)

        return province_population_df
