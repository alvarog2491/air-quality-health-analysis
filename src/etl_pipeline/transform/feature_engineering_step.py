from typing import Any, Dict

import pandas as pd

from etl_pipeline import ETLStep


class FeatureEngineeringStep(ETLStep):
    """Step to perform feature engineering."""

    def __init__(self):
        super().__init__(__name__)

    def execute(
        self, dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]
    ) -> None:
        """Apply feature engineering to merged dataset."""
        self.log_start()

        if "output_df" not in dataframes:
            raise ValueError(
                (
                    "'output_df' is missing in the dataframes dictionary. "
                    "Make sure the merging step has been executed before"
                    "feature engineering."
                )
            )

        # Create respiratory diseases per 100k population column
        self._respiratory_deaths_per_100k(dataframes["output_df"])

        self.log_success(
            f"Features engineered: "
            f"{len(dataframes['output_df'].columns)} total columns"
        )

    def _respiratory_deaths_per_100k(self, df: pd.DataFrame) -> None:
        """Calculate respiratory deaths per 100k population."""
        if (
            "Respiratory_diseases_total" in df.columns
            and "Population" in df.columns
        ):
            df["respiratory_deaths_per_100k"] = round(
                (df["Respiratory_diseases_total"] / df["Population"]) * 100000,
                2,
            )
            self.logger.info("Calculated respiratory_deaths_per_100k")
