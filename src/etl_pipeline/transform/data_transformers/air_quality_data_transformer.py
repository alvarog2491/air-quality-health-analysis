import logging
from typing import Any, Tuple

import pandas as pd

from etl_pipeline.utils.air_quality_rules import (
    quality_labels,
    quality_thresholds,
)

from .base_transformer import BaseTransformer


class AirQualityDataTransformer(BaseTransformer):
    """
    Applies transformations to air quality data:
    - Cleans invalid province values
    - Classifies air quality based on pollutant thresholds
    - Standardizes province names
    """

    def __init__(self):
        """
        Initialize the transformer and set up the logger.
        """
        self.logger = logging.getLogger(self.__class__.__name__)

    def transform(self, *df: pd.DataFrame) -> Tuple[pd.DataFrame, ...]:
        """
        Apply all transformations to the air quality DataFrame.

        Args:
            df (pd.DataFrame): Input DataFrame (expects one).

        Returns:
            Tuple[pd.DataFrame]: Transformed air quality DataFrame.
        """
        air_quality_df: pd.DataFrame = df[0]

        if air_quality_df.empty:
            raise ValueError("Input DataFrame is empty")

        self._convert_invalid_values_to_nan(
            df=air_quality_df,
            column="Province",
            invalid_values=["nan", "Desconocido", "Error"],
        )
        self._classify_quality(air_quality_df)
        self._map_province_names(air_quality_df)

        return (air_quality_df,)

    def _classify_quality(self, air_quality_df: pd.DataFrame) -> pd.DataFrame:
        """
        Assign air quality classification based on pollutant levels.

        Args:
            air_quality_df (pd.DataFrame): Air quality data.

        Returns:
            pd.DataFrame: DataFrame with new 'Quality' column added.
        """
        if "Quality" in air_quality_df.columns:
            self.logger.info("Quality classification already exists, skipping")
            return air_quality_df

        air_quality_df["Air Pollutant"] = air_quality_df[
            "Air Pollutant"
        ].str.lower()

        def get_quality(row: pd.Series) -> str:
            pollutant = row["Air Pollutant"]
            value = row["Air Pollution Level"]
            if pollutant in quality_thresholds:
                bins = quality_thresholds[pollutant]
                label: Any = pd.cut(  # type: ignore
                    [value], bins=bins, labels=quality_labels
                )[0]
                return str(label)
            return "UNKNOWN"

        air_quality_df["Quality"] = air_quality_df.apply(get_quality, axis=1)

        air_quality_df["Air Pollutant"] = air_quality_df[
            "Air Pollutant"
        ].astype("category")
        air_quality_df["Quality"] = air_quality_df["Quality"].astype(
            "category"
        )

        quality_counts = air_quality_df["Quality"].value_counts()
        self.logger.info(
            f"Air quality classification completed: "
            f"{quality_counts.to_dict()}"  # type: ignore
        )

        unknown_count = quality_counts.get("UNKNOWN", 0)
        if unknown_count > 0:
            unknown_pct = (unknown_count / len(air_quality_df)) * 100
            self.logger.warning(
                f"{unknown_count:,} records ({unknown_pct:.1f}%) could not be "
                f"classified (UNKNOWN)"
            )

        return air_quality_df
