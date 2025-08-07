from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd

from common.utils.file_utils import load_yaml_config
from etl_pipeline import ETLStep


class DataValidationStep(ETLStep):
    """
    Enhanced data validation step with comprehensive validation capabilities.
    """

    def __init__(self):
        super().__init__(__name__)
        # Try to load configuration, fall back to defaults if not available
        try:
            from config.config_manager import get_config

            self.config = get_config()
            self.validation_config = self.config.get_validation_config()
            self.processing_config = self.config.get_processing_config()
        except ImportError:
            self.logger.warning(
                "Configuration manager not available, using legacy validation"
            )
            self.config = None
            self.validation_config = {}
            self.processing_config = {}
            self.recovery_enabled = False  # Disable recovery if no config

    def execute(
        self, dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]
    ) -> None:
        """
        Run comprehensive validations on 'output_df'.

        Raises:
            ValueError: If 'output_df' missing or critical validations fail.
        """
        self.log_start()
        if "output_df" not in dataframes:
            raise ValueError(
                "'output_df' is missing. Run feature engineering before "
                "validation."
            )

        df = dataframes["output_df"]

        # Run all validations
        validation_results = self._run_comprehensive_validation(df)

        # Store validation summary in context for potential use by
        # reporting step
        context["validation_summary"] = {
            "passed": validation_results["passed"],
            "error_count": len(validation_results["errors"]),
            "warning_count": len(validation_results["warnings"]),
            "validation_timestamp": validation_results["validation_timestamp"],
        }

        # Check if validation passed
        if not validation_results["passed"]:
            error_messages = "; ".join(validation_results["errors"])
            raise ValueError(f"Dataset validation failed: {error_messages}")

        # Log warnings if any
        if validation_results["warnings"]:
            for warning in validation_results["warnings"]:
                self.logger.warning(warning)

        self.log_success(
            f"Dataset validation passed with "
            f"{len(validation_results['warnings'])} warnings"
        )

    def _run_comprehensive_validation(
        self, df: pd.DataFrame
    ) -> Dict[str, Any]:
        """Run comprehensive validation on the DataFrame."""
        results: Dict[str, Any] = {
            "df_name": "output_df",
            "total_records": len(df),
            "validation_timestamp": datetime.now(),
            "passed": True,
            "warnings": [],
            "errors": [],
        }

        try:
            # Basic validations (keep existing behavior for compatibility)
            self._validate_not_empty(df, results)
            self._validate_nulls(df, results)
            self._validate_dtypes(df, results)
            self._validate_duplicates(df, results)

            # Enhanced validations (only if config available)
            if self.config:
                self._validate_required_columns(df, results)
                self._validate_business_rules(df, results)
                self._detect_statistical_anomalies(df, results)

        except Exception as e:
            results["passed"] = False
            results["errors"].append(
                f"Validation failed with exception: {str(e)}"
            )
            self.logger.error(f"Validation failed: {str(e)}")

        return results

    def _validate_not_empty(
        self, df: pd.DataFrame, results: Dict[str, Any]
    ) -> None:
        """Validate that DataFrame is not empty."""
        if df.empty:
            results["passed"] = False
            results["errors"].append("DataFrame is empty")
        elif len(df) == 0:
            results["passed"] = False
            results["errors"].append("DataFrame has no rows")

    def _validate_nulls(
        self, df: pd.DataFrame, results: Dict[str, Any]
    ) -> None:
        """Enhanced null validation with configurable thresholds."""
        null_counts = df.isnull().sum()
        total_nulls = null_counts.sum()

        if total_nulls > 0:
            if self.config:
                # Use configuration-based validation
                null_percentage = (
                    total_nulls / (len(df) * len(df.columns))
                ) * 100
                max_null_percent = self.processing_config.get(
                    "data_quality", {}
                ).get("null_threshold_percent", 0.0)

                if null_percentage > max_null_percent:
                    results["passed"] = False
                    results["errors"].append(
                        f"Too many null values: {null_percentage:.2f}% "
                        f"(max allowed: {max_null_percent}%)"
                    )
                else:
                    results["warnings"].append(
                        f"Found {total_nulls} null values "
                        f"({null_percentage:.2f}%)"
                    )
            else:
                # Legacy strict validation (original behavior)
                results["passed"] = False
                results["errors"].append("Dataset contains null values")
        else:
            self.logger.info("No null values found")

    def _validate_dtypes(
        self, df: pd.DataFrame, results: Dict[str, Any]
    ) -> None:
        """Enhanced data type validation."""
        try:
            config_path = (
                Path(__file__).parent.parent.parent
                / "common"
                / "feature_types.yaml"
            )
            feature_config = load_yaml_config(config_path)
            expected_dtypes = feature_config.get("preprocess", {}).get(
                "var_dtypes", {}
            )

            for col, expected_dtype in expected_dtypes.items():
                if col in df.columns:
                    actual_dtype = str(df[col].dtype)  # type: ignore
                    if actual_dtype != expected_dtype:
                        if self.config:
                            results["warnings"].append(
                                f"Column '{col}' has dtype '{actual_dtype}' "
                                f"instead of '{expected_dtype}'"
                            )
                        else:
                            results["passed"] = False
                            results["errors"].append(
                                f"Column '{col}' has dtype '{actual_dtype}' "
                                f"instead of '{expected_dtype}'"
                            )

            if not results["errors"]:
                self.logger.info("All columns have correct data types")

        except Exception as e:
            results["warnings"].append(f"Could not validate data types: {e}")

    def _validate_duplicates(
        self, df: pd.DataFrame, results: Dict[str, Any]
    ) -> None:
        """Enhanced duplicate validation with configuration support."""
        duplicate_count = df.duplicated().sum()

        if duplicate_count > 0:
            if self.config:
                allow_duplicates = self.processing_config.get(
                    "data_quality", {}
                ).get("allow_duplicates", False)
                if not allow_duplicates:
                    results["passed"] = False
                    results["errors"].append(
                        f"Found {duplicate_count} duplicate rows"
                    )
                else:
                    results["warnings"].append(
                        f"Found {duplicate_count} duplicate rows "
                        f"(allowed by configuration)"
                    )
            else:
                # Legacy strict validation
                results["passed"] = False
                results["errors"].append("Dataset contains duplicated rows")
        else:
            self.logger.info("No duplicated rows found")

    def _validate_required_columns(
        self, df: pd.DataFrame, results: Dict[str, Any]
    ) -> None:
        """Validate that required columns are present."""
        required_columns = self.validation_config.get("required_columns", [])
        if not required_columns:
            return

        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            results["passed"] = False
            results["errors"].append(
                f"Missing required columns: " f"{sorted(missing_columns)}"
            )

    def _validate_business_rules(
        self, df: pd.DataFrame, results: Dict[str, Any]
    ) -> None:
        """Validate business-specific rules."""
        # Validate time range
        if "Year" in df.columns:
            time_range = self.processing_config.get("time_range", {})
            start_year = time_range.get("start_year", 2000)
            end_year = time_range.get("end_year", 2021)

            invalid_years = df[  # type: ignore
                (df["Year"].dt.year < start_year)
                | (df["Year"].dt.year > end_year)
            ]
            if len(invalid_years) > 0:  # type: ignore
                results["warnings"].append(
                    (
                        f"Found {len(invalid_years)} records "  # type: ignore
                        f"with years outside valid range "
                        f"({start_year}-{end_year})"
                    )
                )

        # Validate air quality levels
        if "Air Pollution Level" in df.columns:
            negative_pollution = df[df["Air Pollution Level"] < 0]
            if len(negative_pollution) > 0:
                results["errors"].append(
                    f"Found {len(negative_pollution)} records with "
                    f"negative pollution levels"
                )
                results["passed"] = False

    def _detect_statistical_anomalies(
        self, df: pd.DataFrame, results: Dict[str, Any]
    ) -> None:
        """Detect statistical anomalies in numeric columns."""
        numeric_columns = df.select_dtypes(include=[np.number]).columns

        for column in numeric_columns:
            if df[column].isna().all():
                continue

            # Calculate IQR for outlier detection
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1

            if IQR == 0:  # Skip if no variation
                continue

            # Define outlier bounds
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            outliers = df[
                (df[column] < lower_bound) | (df[column] > upper_bound)
            ]
            outlier_count = len(outliers)

            if outlier_count > 0:
                outlier_percentage = (outlier_count / len(df)) * 100
                if (
                    outlier_percentage > 10
                ):  # Only warn if significant outlier percentage
                    results["warnings"].append(
                        f"Column '{column}': {outlier_count} outliers "
                        f"detected ({outlier_percentage:.1f}%)"
                    )
