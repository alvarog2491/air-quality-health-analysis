from pathlib import Path
from typing import Any, Dict

import pandas as pd

# from load.data_reporters import AirQualityDataReporter
from pandas.core.indexes.base import Index

from etl_pipeline import ETLStep


class DataQualityReportStep(ETLStep):
    """Step to generate and persist a data quality report for the cleaned
    dataset."""

    def __init__(self):
        super().__init__(__name__)

    def execute(
        self, dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]
    ) -> None:
        """
        Generate and save a comprehensive data quality report.

        Args:
            dataframes (Dict[str, pd.DataFrame]): Dictionary containing
                pipeline dataframes.
            context (Dict[str, Any]): Execution context with metadata and
                paths.

        Raises:
            ValueError: If required keys are missing in dataframes or context.
        """
        self.log_start()
        self._validate_args(dataframes, context)

        df = dataframes["output_df"]
        self._output_dir = Path(context["data_path"]) / "output" / "reports"
        self._report_file = self._output_dir / "data_quality_report.json"

        context["quality_report_path"] = str(self._report_file)
        context["reports_path"] = str(self._output_dir)

        self._generate_report(df)

    def _validate_args(
        self, dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]
    ) -> None:
        """
        Validate presence of required keys in dataframes and context.

        Args:
            dataframes: Dict of available DataFrames.
            context: Context dict with runtime info.

        Raises:
            ValueError: If 'output_df' or 'data_path' is missing.
        """
        if "output_df" not in dataframes:
            raise ValueError(
                "'output_df' missing. Run feature engineering before "
                "reporting."
            )
        if not context.get("data_path"):
            raise ValueError(
                "'data_path' missing in context. Check project structure "
                "setup."
            )

    def _generate_report(self, df: pd.DataFrame) -> None:
        """
        Compile data quality metrics and save report.

        Args:
            df: DataFrame to analyze.
        """
        report: Dict[str, Any] = {
            "total_records": len(df),
            "total_columns": len(df.columns),
            "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
            "duplicate_rows": df.duplicated().sum(),
            "missing_data": self._missing_data_stats(df),
            "data_types": (
                df.dtypes.astype(str).value_counts().to_dict()  # type: ignore
            ),
            "year_statistics": self._year_statistics(df),
        }

        # Numeric column summary
        numeric_cols: Index[str] = df.select_dtypes(include=["number"]).columns
        if not numeric_cols.empty:
            report["numeric_summary"] = (
                df[numeric_cols].describe().to_dict()  # type: ignore
            )

        # Categorical column summary
        categorical_cols = df.select_dtypes(
            include=["object", "category"]
        ).columns
        if not categorical_cols.empty:
            report["categorical_summary"] = {
                col: {
                    "unique_values": df[col].nunique(),
                    "most_frequent": (
                        df[col].mode().iloc[0]
                        if not df[col].mode().empty
                        else None
                    ),
                }
                for col in categorical_cols
            }

        self._save_report(report)

    def _year_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Compute year-related statistics from the dataset.

        Args:
            df: DataFrame to analyze.

        Returns:
            Dict[str, Any]: Statistics including min/max year and year counts.
        """
        if "Year" not in df.columns:
            return {
                "error": "Year column not found in dataset"
            }
        
        # Convert Year column to datetime if it's not already
        year_series = pd.to_datetime(df["Year"])
        years = year_series.dt.year
        
        # Calculate year statistics
        min_year = int(years.min())
        max_year = int(years.max())
        year_counts = years.value_counts().sort_index().to_dict()
        
        return {
            "min_year": min_year,
            "max_year": max_year,
            "total_years": len(year_counts),
            "year_counts": {int(year): int(count) for year, count in year_counts.items()}
        }

    def _missing_data_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Compute missing data statistics.

        Args:
            df: DataFrame to evaluate.

        Returns:
            Dict[str, Any]: Summary of missing value counts and percentages.
        """
        missing = df.isnull().sum()
        total_cells = len(df) * len(df.columns)
        total_missing = missing.sum()
        return {
            "total_missing_values": total_missing,
            "columns_with_missing": (missing > 0).sum(),
            "missing_percentage": (total_missing / total_cells) * 100,
        }

    def _save_report(self, report: Dict[str, Any]) -> None:
        """
        Save the generated report to a JSON file.

        Args:
            report: Dictionary containing quality metrics.
        """
        import json

        self._output_dir.mkdir(parents=True, exist_ok=True)
        with open(self._report_file, "w") as f:
            json.dump(report, f, indent=2, default=str)

        self.logger.info(f"Quality report saved to {self._report_file}")
