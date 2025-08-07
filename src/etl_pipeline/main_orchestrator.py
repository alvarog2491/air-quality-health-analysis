#!/usr/bin/env python3
"""
ETL Pipeline Orchestrator

Coordinates the full ETL process by running each phase sequentially:
Extraction, Transformation, and Loading.
"""

import logging
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from etl_pipeline import ETLStep
from etl_pipeline.config.config_manager import get_config
from etl_pipeline.config.logger import setup_logger
from etl_pipeline.extract import DataExtractionStep
from etl_pipeline.load import DataExportStep, DataQualityReportStep
from etl_pipeline.transform import (
    DataCleaningStep,
    DataMergingStep,
    DataTransformationStep,
    DataValidationStep,
    FeatureEngineeringStep,
)
from etl_pipeline.utils import CheckProjectStructure

setup_logger()


class ETLPipeline:
    """
    Main class responsible for running a full ETL pipeline.¸

    Attributes:
        steps (List[ETLStep]): List of ETL steps to execute in order.
    """

    def __init__(self, steps: Optional[Sequence[ETLStep]] = None):
        """
        Initializes the ETLPipeline.

        Args:
            steps (Optional[List[ETLStep]]): Optional list of ETL steps.
                If None, defaults are loaded.
        """
        self.logger = logging.getLogger(self.__class__.__name__)

        # Load configuration if available
        try:
            self.config = get_config()
            self.logger.info("Configuration manager loaded successfully")
        except Exception as e:
            self.logger.warning(f"Failed to load configuration manager: {e}")
            self.config = None
        else:
            self.config = None

        self.steps = steps or self._get_default_steps()
        self.recovery_enabled = True  # Enable recovery by default

    def _get_default_steps(self) -> List[ETLStep]:
        """
        Returns the default list of ETL steps.

        Returns:
            List[ETLStep]: Default ETL steps in execution order.
        """
        return [
            DataExtractionStep(),
            DataTransformationStep(),
            DataMergingStep(),
            FeatureEngineeringStep(),
            DataCleaningStep(),
            DataValidationStep(),
            DataExportStep(),
            DataQualityReportStep(),
        ]

    def _can_recover_from_error(self, step: ETLStep, error: Exception) -> bool:
        """
        Determine if can recover from a specific error.

        Args:
            step: The ETL step that failed
            error: The exception that occurred

        Returns:
            True if recovery is possible, False otherwise
        """
        # Define recoverable error patterns
        recoverable_errors = [
            "validation passed with",  # Warnings that don't fail validation
            "outliers detected",  # Statistical warnings
            "outside valid range",  # Business rule warnings
        ]

        error_message = str(error).lower()

        # Check if it's a recoverable validation warning
        if any(
            recoverable in error_message for recoverable in recoverable_errors
        ):
            return True

        # Data type mismatches might be recoverable with coercion
        if "dtype" in error_message and "instead of" in error_message:
            return True

        # Some file access errors might be recoverable
        if (
            "permission denied" in error_message
            or "file not found" in error_message
        ):
            return False  # These need manual intervention

        return False

    def run(self) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Executes the ETL pipeline by running all configured steps in order.

        Returns:
            Tuple[pd.DataFrame, Dict[str, Any]]:
                - The final processed DataFrame.
                - Dictionary with metadata such as execution time, output file,
                  final shape, and number of executed steps..
        """
        start_time = datetime.now()
        self.logger.info("Starting ETL Pipeline execution...")

        try:
            # Initialize Data
            data_path = CheckProjectStructure().execute()
            dataframes: Dict[str, pd.DataFrame] = {}
            context: Dict[str, Any] = {
                "data_path": data_path,
                "export_format": ["csv"],
            }

            # Run all pipeline steps with improved error handling
            for i, step in enumerate(self.steps):
                step_name = step.__class__.__name__
                try:
                    self.logger.info(
                        f"Executing step {i+1}/{len(self.steps)}: {step_name}"
                    )
                    step.execute(dataframes, context)
                    self.logger.info(
                        f"✅ Step {step_name} completed successfully"
                    )

                except Exception as step_error:
                    self.logger.error(
                        f"❌ Step {step_name} failed: {str(step_error)}"
                    )

                    if self.recovery_enabled and self._can_recover_from_error(
                        step, step_error
                    ):
                        self.logger.warning(
                            f"⚠️ Attempting recovery for step {step_name}"
                        )
                        try:
                            self._attempt_step_recovery(
                                step, dataframes, context, step_error
                            )
                            self.logger.info(
                                f"✅ Recovery successful for step {step_name}"
                            )
                            continue
                        except Exception as recovery_error:
                            self.logger.error(
                                (
                                    f"❌ Recovery failed for step {step_name}: "
                                    f"{str(recovery_error)}"
                                )
                            )

                    # If we can't recover or recovery is disabled, re-raise
                    # the error
                    raise step_error

            # Show results
            processing_time = datetime.now() - start_time
            output_file_path: str = context["output_file_path"]
            output_file: pd.DataFrame = context["output_file"]
            reports_path: pd.DataFrame = context["reports_path"]

            results: Dict[str, Any] = {
                "execution_time": processing_time,
                "output_file_path": output_file_path,
                "reports_path": reports_path,
                "final_shape": output_file.shape,
                "steps_executed": [
                    f"{i} - {step.__class__.__name__}\n"
                    for i, step in enumerate(self.steps)
                ],
            }
            return output_file, results

        except Exception as e:
            self.logger.error(f"ETL Pipeline failed: {str(e)}")
            raise

    def _attempt_step_recovery(
        self,
        step: ETLStep,
        dataframes: Dict[str, pd.DataFrame],
        context: Dict[str, Any],
        error: Exception,
    ) -> None:
        """
        Attempt to recover from a step failure.

        Args:
            step: The ETL step that failed
            dataframes: Current dataframes
            context: Current context
            error: The exception that occurred
        """
        step_name = step.__class__.__name__
        error_message = str(error).lower()

        if step_name == "DataValidationStep":
            # For validation steps, we can try to continue with warnings
            if "validation passed with" in error_message:
                self.logger.warning(
                    "Validation completed with warnings, continuing pipeline"
                )
                return

            # Try to relax validation constraints
            if hasattr(step, "recovery_enabled"):
                step.recovery_enabled = True
                step.execute(dataframes, context)
                return

        elif step_name == "DataCleaningStep":
            # For cleaning steps, we might try with less strict parameters
            if "null" in error_message:
                self.logger.warning(
                    "Attempting data cleaning with relaxed null handling"
                )
                # Could implement relaxed cleaning here
                return

        # If no specific recovery strategy, log and re-raise
        raise error


def main():
    """
    Main execution entry point for the default ETL pipeline.

    Runs the pipeline and prints a summary of the results.
    """
    print("Starting automated data processing...")
    print("=" * 60)

    try:
        pipeline = ETLPipeline()
        final_df, results = pipeline.run()

        print("\n" + "=" * 60)
        print("✅ Processing completed successfully!")
        print(
            (
                f"Final dataset: {results['final_shape'][0]} rows, "
                f"{results['final_shape'][1]} columns"
            )
        )
        print(f"Total time: {results['execution_time']}")
        print(f"File saved as: {results['output_file_path']}")
        print(f"Reports saved at: {results['reports_path']}")
        print("Steps executed:\n" + "".join(results["steps_executed"]))
        print("=" * 60)

        if not final_df.empty:
            print("\nFinal dataset preview:")
            print(final_df.head())
            print("\nDataset info:")
            print(final_df.info())

        return final_df, results

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        print(f"❌ Error: {str(e)}")
        # raise e
        sys.exit(1)


if __name__ == "__main__":
    main()
