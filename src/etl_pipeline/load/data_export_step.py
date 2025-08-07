from typing import Any, Dict, List

import pandas as pd

from etl_pipeline import ETLStep


class DataExportStep(ETLStep):
    """
    ETL step responsible for exporting the dataset into multiple file formats.

    Requires:
        - 'output_df' in the dataframes dictionary as the DataFrame to export.
        - 'data_path' in the context dictionary as the base path for
          output files.
        - 'export_format' in the context dictionary as a list of formats
          to export.

    Supported export formats include 'csv' and 'parquet'.
    """

    def __init__(self):
        """
        Initialize the DataExportStep.
        """
        super().__init__(__name__)

    def execute(
        self, dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]
    ) -> None:
        """
        Export the dataset in one or more formats as specified in the context.

        Args:
            dataframes (Dict[str, pd.DataFrame]): Dictionary containing
                dataframes,
                must include 'output_df'.
            context (Dict[str, Any]): Dictionary with execution context,
                must include
                'data_path' and 'export_format'.

        Raises:
            ValueError: If 'output_df' is missing or empty.
            ValueError: If 'data_path' or 'export_format' are missing in
                the context.
        """
        self.log_start()
        self._validate_arguments(dataframes, context)

        output_df: pd.DataFrame = dataframes["output_df"]
        export_formats: List[str] = context["export_format"]
        output_dir = context["data_path"] / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        exported_files: Dict[str, str] = {}
        output_file_path = ""

        for format_type in export_formats:
            if format_type == "csv":
                output_file_path = output_dir / "dataset.csv"
                output_df.to_csv(output_file_path, index=False)
            elif format_type == "parquet":
                output_file_path = output_dir / "dataset.parquet"
                output_df.to_parquet(output_file_path)
            else:
                self.logger.warning(
                    f"Unsupported export format: {format_type}"
                )
                continue

            exported_files[format_type] = str(output_file_path)
            self.logger.info(
                f"Exported dataset to {format_type}: {output_file_path}"
            )

        self.log_success(
            f"Export completed in {len(exported_files)} format(s)"
        )
        context["output_file_path"] = output_file_path
        context["output_file"] = output_df

    def _validate_arguments(
        self, dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]
    ) -> None:
        """
        Validate presence and correctness of required arguments before export.

        Args:
            dataframes (Dict[str, pd.DataFrame]): Dictionary containing
                dataframes.
            context (Dict[str, Any]): Execution context.

        Raises:
            ValueError: If 'output_df' is missing or empty.
            ValueError: If 'data_path' or 'export_format' are missing in
                context.
        """
        if "output_df" not in dataframes:
            raise ValueError(
                "'output_df' is missing in the dataframes dictionary. "
                "Ensure the feature engineering step has been executed "
                "before export."
            )
        if dataframes["output_df"].empty:
            raise ValueError(
                "The input DataFrame 'output_df' is empty; nothing to "
                "export."
            )

        if not context.get("data_path"):
            raise ValueError(
                "'data_path' is missing in the context. "
                "This may indicate the project structure was not "
                "verified. "
                "Refer to 'utils.check_project_structure' for "
                "instructions."
            )
        if not context.get("export_format"):
            raise ValueError(
                "'export_format' is missing in the context. "
                "Please configure export formats in the main "
                "orchestrator."
            )
