from typing import Any, Dict
import pandas as pd

from etl_pipeline.transform.data_transformers import (
    AirQualityDataTransformer,
    HealthDataTransformer,
    SocioeconomicDataTransformer,
)
from etl_pipeline import ETLStep


class DataTransformationStep(ETLStep):
    """
    ETL step that applies transformations to all extracted datasets.

    This step delegates the transformation to specific transformer classes
    for air quality, health, and socioeconomic data.
    """

    def __init__(self):
        """
        Initialize the transformation step with a descriptive name.
        """
        super().__init__("Data Transformation")

    def execute(
        self, dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]
    ) -> None:
        """
        Apply transformations to all extracted data.

        Args:
            dataframes (Dict[str, pd.DataFrame]): Dictionary containing raw
                DataFrames to transform.
            context (Dict[str, Any]): Additional context (not used here).
        """
        self.log_start()

        # Air quality transformation
        self.logger.info("Transforming air quality data...")
        (air_quality_df,) = AirQualityDataTransformer().transform(
            dataframes["air_quality"]
        )
        dataframes["air_quality"] = air_quality_df

        # Health data transformation
        self.logger.info("Transforming health data...")
        (
            respiratory_diseases_df,
            life_expectancy_df,
        ) = HealthDataTransformer().transform(
            dataframes["respiratory_diseases"], dataframes["life_expectancy"]
        )
        dataframes["respiratory_diseases"] = respiratory_diseases_df
        dataframes["life_expectancy"] = life_expectancy_df

        # Socioeconomic data transformation
        self.logger.info("Transforming socioeconomic data...")
        gdp_df, population_df = SocioeconomicDataTransformer().transform(
            dataframes["gdp"], dataframes["province_population"]
        )
        dataframes["gdp"] = gdp_df
        dataframes["province_population"] = population_df

        self.log_success(f"Transformed {len(dataframes)} datasets")
