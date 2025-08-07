"""
Data extraction step for all data sources.
"""

from typing import Any, Dict

import pandas as pd
from etl_pipeline.extract.data_extractors import (
    AirQualityDataExtractor,
    HealthDataExtractor,
    SocioeconomicDataExtractor,
)

from etl_pipeline import ETLStep


class DataExtractionStep(ETLStep):
    """
    ETL step responsible for extracting raw datasets from all data sources.
    """

    def __init__(self):
        """
        Initialize the data extraction step.
        """
        super().__init__(__name__)

    def execute(
        self, dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]
    ) -> None:
        """
        Extract air quality, health, and socioeconomic datasets.

        Args:
            dataframes (Dict[str, pd.DataFrame]): Dictionary to store the
                extracted datasets.
            context (Dict[str, Any]): Execution context containing
                configuration parameters.

        Raises:
            ValueError: If 'data_path' is missing from the context.
        """
        self.log_start()

        if not context.get("data_path"):
            raise ValueError(
                "'data_path' is missing in the context. "
                "Ensure the project structure has been verified. "
                "Refer to 'utils.check_project_structure' for setup."
            )
        data_path = context["data_path"]

        # Air Quality Data
        self.logger.info("Extracting air quality data...")
        AirQualityDataExtractor(data_path).extract(dataframes)

        # Health Data
        self.logger.info("Extracting health data...")
        HealthDataExtractor(data_path).extract(dataframes)

        # Socioeconomic Data
        self.logger.info("Extracting socioeconomic data...")
        SocioeconomicDataExtractor(data_path).extract(dataframes)

        self.log_success(f"Extracted {len(dataframes)} datasets")
