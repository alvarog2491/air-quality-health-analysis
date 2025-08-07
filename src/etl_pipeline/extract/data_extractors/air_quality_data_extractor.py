from pathlib import Path
from typing import Dict

import pandas as pd

from etl_pipeline.config.config_manager import get_config

from .base_extractor import BaseExtractor


class AirQualityDataExtractor(BaseExtractor):
    """
    Extractor for air quality data from CSV files.

    Loads predefined columns from a raw air quality dataset.
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
            "data_sources.air_quality.data_directory", "air_quality_data"
        )
        self._raw_file: str = self.config.get(
            "data_sources.air_quality.raw_file",
            "air_quality_with_province.csv",
        )
        self._cols_to_use: list[str] = self.config.get(
            "data_sources.air_quality.columns_to_use", []
        )
        self._format = self.config.get(
            "data_sources.air_quality.format", "csv"
        )

    def extract(self, dataframes: Dict[str, pd.DataFrame]) -> None:
        """
        Extract air quality data and store it in the dataframes dictionary.

        Args:
            dataframes (Dict[str, pd.DataFrame]): Dictionary to store the
                extracted DataFrame.
            format (str): File format to read from (default is 'csv').
        """
        if self._format == "csv":
            dataframes["air_quality"] = self._read_csv_files()

    def _read_csv_files(self) -> pd.DataFrame:
        """
        Read the air quality CSV file and return the loaded DataFrame.

        Returns:
            pd.DataFrame: Loaded air quality data.

        Raises:
            FileNotFoundError: If the CSV file does not exist.
            ValueError: If the CSV file is empty or cannot be read.
        """
        self.logger.info(
            f"Loading raw air quality data from: {self.data_path}"
        )
        file_path = (
            self.data_path
            / self._data_directory
            / self.raw_folder
            / self._raw_file
        )

        if not file_path.is_file():
            raise FileNotFoundError(f"Required file not found: {file_path}")

        try:
            df: pd.DataFrame = pd.read_csv(  # type: ignore
                file_path, usecols=self._cols_to_use, parse_dates=["Year"]
            )
            self._log_dataframe_info(df)
            return df
        except Exception as e:
            self.logger.error(f"Error loading CSV file: {str(e)}")
            raise
