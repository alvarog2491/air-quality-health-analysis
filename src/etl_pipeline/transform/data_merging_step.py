from typing import Any, Dict

import pandas as pd

from etl_pipeline import ETLStep


class DataMergingStep(ETLStep):
    """Merge all transformed datasets into one."""

    def __init__(self):
        super().__init__(__name__)

    def execute(
        self, dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]
    ) -> None:
        """Validate and merge datasets."""
        self.log_start()

        air_quality = dataframes["air_quality"]
        respiratory = dataframes["respiratory_diseases"]
        life_expectancy = dataframes["life_expectancy"]
        gdp = dataframes["gdp"]
        population = dataframes["province_population"]

        self._validate_merge_columns(
            air_quality, respiratory, life_expectancy, gdp, population
        )

        merged_df = self._merge_all_data(
            air_quality, respiratory, life_expectancy, gdp, population
        )

        dataframes["output_df"] = merged_df

        self.log_success(
            f"Merged {len(merged_df)} records with "
            f"{len(merged_df.columns)} columns"
        )

    def _merge_all_data(
        self,
        air_quality: pd.DataFrame,
        respiratory: pd.DataFrame,
        life_expectancy: pd.DataFrame,
        gdp: pd.DataFrame,
        population: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge datasets on ['Province', 'Year'] and drop redundant
        columns."""
        merged = (
            air_quality.merge(respiratory, on=["Province", "Year"], how="left")
            .merge(life_expectancy, on=["Province", "Year"], how="left")
            .merge(gdp, on=["Province", "Year"], how="left")
            .merge(population, on=["Province", "Year"], how="left")
        )

        merged.drop(
            columns=["Causa de muerte", "Sexo", "Sexo_x", "Sexo_y"],
            inplace=True,
            errors="ignore",
        )
        return merged

    def _validate_merge_columns(
        self,
        air_quality: pd.DataFrame,
        respiratory: pd.DataFrame,
        life_expectancy: pd.DataFrame,
        gdp: pd.DataFrame,
        population: pd.DataFrame,
    ) -> None:
        """Check required columns exist in each dataset."""
        required_cols = ["Province", "Year"]
        datasets = [
            (air_quality, "Air Quality"),
            (respiratory, "Respiratory Diseases"),
            (life_expectancy, "Life Expectancy"),
            (gdp, "GDP"),
            (population, "Province Population"),
        ]

        for df, name in datasets:
            self._check_columns(df, required_cols, name)

    def _check_columns(
        self, df: pd.DataFrame, required: list[str], name: str
    ) -> None:
        """Raise ValueError if any required column is missing."""
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"{name} DataFrame missing columns: {missing}")
