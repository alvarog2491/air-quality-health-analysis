from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

from etl_pipeline.config.config_manager import get_config

from .base_extractor import BaseExtractor


class HealthDataExtractor(BaseExtractor):
    """
    Extractor for health-related data: respiratory diseases and life
    expectancy.

    Loads and returns two separate DataFrames from raw CSV files.
    """

    _COLUMN_DTYPES: Dict[str, str] = {
        "Causa de muerte": "category",
        "Sexo": "category",
        "Provincias": "category",
        "Total": "float64",
    }

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
            "data_sources.health.data_directory", "health_data"
        )
        self._respiratory_file: str = self.config.get(
            "data_sources.health.respiratory_diseases_file",
            "enfermedades_respiratorias.csv",
        )
        self._life_expectancy_file: str = self.config.get(
            "data_sources.health.life_expectancy_file",
            "esperanza_vida.csv",
        )
        self._format = self.config.get("data_sources.health.format", "csv")
        self._respiratory_separator = self.config.get(
            "data_sources.health.respiratory_separator", ";"
        )
        self._respiratory_decimal = self.config.get(
            "data_sources.health.respiratory_decimal", ","
        )
        self._life_expectancy_separator = self.config.get(
            "data_sources.health.life_expectancy_separator", ";"
        )
        self._life_expectancy_decimal = self.config.get(
            "data_sources.health.life_expectancy_decimal", ","
        )
        self._life_expectancy_encoding = self.config.get(
            "data_sources.health.life_expectancy_encoding", "latin1"
        )
        self._date_columns = self.config.get(
            "data_sources.health.date_columns", ["Periodo"]
        )

    def extract(self, dataframes: Dict[str, pd.DataFrame]) -> None:
        """
        Extract health datasets and store them in the dataframes dictionary.

        Args:
            dataframes (Dict[str, pd.DataFrame]): Dictionary to store
                extracted DataFrames.
        """
        if self._format == "csv":
            respiratory_df, life_expectancy_df = self._read_csv_files()
            dataframes["respiratory_diseases"] = respiratory_df
            dataframes["life_expectancy"] = life_expectancy_df

    def _read_csv_files(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Read raw health data from CSV files.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: DataFrames for respiratory
                diseases and life expectancy.

        Raises:
            FileNotFoundError: If any required file is missing.
            Exception: If any file fails to load.
        """
        self.logger.info(f"Loading raw health data from: {self.data_path}")

        respiratory_file = (
            self.data_path
            / self._data_directory
            / self.raw_folder
            / self._respiratory_file
        )
        life_expectancy_file = (
            self.data_path
            / self._data_directory
            / self.raw_folder
            / self._life_expectancy_file
        )

        if not respiratory_file.is_file():
            raise FileNotFoundError(
                f"Required file not found: {respiratory_file}"
            )
        if not life_expectancy_file.is_file():
            raise FileNotFoundError(
                f"Required file not found: {life_expectancy_file}"
            )

        try:
            respiratory_df = pd.read_csv(  # type: ignore
                respiratory_file,
                parse_dates=self._date_columns,
                decimal=self._respiratory_decimal,
                sep=self._respiratory_separator,
            )

            life_expectancy_df = pd.read_csv(  # type: ignore
                life_expectancy_file,
                parse_dates=self._date_columns,
                decimal=self._life_expectancy_decimal,
                sep=self._life_expectancy_separator,
                encoding=self._life_expectancy_encoding,
            )

            self._log_dataframe_info(respiratory_df)
            self._log_dataframe_info(life_expectancy_df)

            return respiratory_df, life_expectancy_df

        except Exception as e:
            self.logger.error(f"Error loading CSV files: {str(e)}")
            raise
