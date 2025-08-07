import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict

import pandas as pd

from common.utils.dataframe_utils import (
    log_duplicated_rows,
    log_empty_rows,
    log_info,
    log_memory_usage,
    log_null_values,
)


class BaseExtractor(ABC):
    """
    Abstract base class for data extractors.
    Provides shared logic for managing file paths and logging DataFrame
    information.
    """

    def __init__(self, name: str, data_path: Path):
        """
        Initialize the extractor.
        """
        self.name = name
        self.data_path = data_path
        self.raw_folder = "raw"
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def extract(self, dataframes: Dict[str, pd.DataFrame]) -> None:
        """
        Abstract method for extracting data into a dictionary of DataFrames.

        Args:
            dataframes: Dictionary to store the extracted DataFrame(s).

        Raises:
            FileNotFoundError: If the required file is not found.
            ValueError: If the extracted DataFrame is empty.
        """
        pass

    def _log_dataframe_info(self, df: pd.DataFrame) -> None:
        """
        Log summary information about the provided DataFrame.

        Includes null values, duplicated rows, empty rows, and overall
        memory usage.

        Args:
            df: DataFrame to log.
        """
        log_null_values(df)
        log_duplicated_rows(df)
        log_empty_rows(df)
        log_info(df)
        log_memory_usage(df)
