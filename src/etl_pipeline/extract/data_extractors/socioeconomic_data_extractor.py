from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

from etl_pipeline.config.config_manager import get_config

from .base_extractor import BaseExtractor


class SocioeconomicDataExtractor(BaseExtractor):
    """
    Extractor for socioeconomic data: GDP per capita and provincial population.

    Loads and returns raw data from two CSV sources.
    """

    def __init__(self, data_path: Path):
        """
        Initialize the extractor with the path to the data directory.
        """
        super().__init__(__name__, data_path=data_path)
        self._process_config()

    def _process_config(self) -> None:
        """
        Process configuration settings for the extractor.
        This method can be extended to handle more complex configurations.
        """
        self.config = get_config()
        self._data_directory: str = self.config.get(
            "data_sources.socioeconomic.data_directory", "socioeconomic_data"
        )
        self._gdp_file: str = self.config.get(
            "data_sources.socioeconomic.gdp_file",
            "PIB per cap provincias 2000-2021.csv",
        )
        self._population_file: str = self.config.get(
            "data_sources.socioeconomic.population_file",
            "poblacion_provincias.csv",
        )
        self._format = self.config.get(
            "data_sources.socioeconomic.format", "csv"
        )
        self._gdp_separator = self.config.get(
            "data_sources.socioeconomic.gdp_separator", ";"
        )
        self._gdp_decimal = self.config.get(
            "data_sources.socioeconomic.gdp_decimal", ","
        )
        self._gdp_encoding = self.config.get(
            "data_sources.socioeconomic.gdp_encoding", "ISO-8859-1"
        )
        self._population_separator = self.config.get(
            "data_sources.socioeconomic.population_separator", ";"
        )
        self._population_decimal = self.config.get(
            "data_sources.socioeconomic.population_decimal", ","
        )
        self._population_encoding = self.config.get(
            "data_sources.socioeconomic.population_encoding", "latin1"
        )
        self._date_columns = self.config.get(
            "data_sources.socioeconomic.date_columns", ["Periodo"]
        )

    def extract(self, dataframes: Dict[str, pd.DataFrame]) -> None:
        """
        Extract socioeconomic datasets and store them in the dataframes
        dictionary.

        Args:
            dataframes (Dict[str, pd.DataFrame]): Dictionary to store
                extracted DataFrames.
        """
        if self._format == "csv":
            gdp_df, population_df = self._read_csv_files()
            dataframes["gdp"] = gdp_df
            dataframes["province_population"] = population_df

    def _read_csv_files(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Read raw GDP and population data from CSV files.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: DataFrames for GDP per capita
                and provincial population.

        Raises:
            FileNotFoundError: If any required file is missing.
            Exception: If any file fails to load.
        """
        self.logger.info(
            f"Loading raw socioeconomic data from: {self.data_path}"
        )

        pib_file = (
            self.data_path
            / self._data_directory
            / self.raw_folder
            / self._gdp_file
        )
        population_file = (
            self.data_path
            / self._data_directory
            / self.raw_folder
            / self._population_file
        )

        if not pib_file.is_file():
            raise FileNotFoundError(f"Required file not found: {pib_file}")
        if not population_file.is_file():
            raise FileNotFoundError(
                f"Required file not found: {population_file}"
            )

        try:
            gdp_df = pd.read_csv(  # type: ignore
                pib_file,
                sep=self._gdp_separator,
                decimal=self._gdp_decimal,
                encoding=self._gdp_encoding,
            )

            population_df = pd.read_csv(  # type: ignore
                population_file,
                parse_dates=self._date_columns,
                sep=self._population_separator,
                decimal=self._population_decimal,
                encoding=self._population_encoding,
            )

            self._log_dataframe_info(gdp_df)
            self._log_dataframe_info(population_df)

            return gdp_df, population_df

        except Exception as e:
            self.logger.error(f"Error loading CSV file: {str(e)}")
            raise
