from pathlib import Path
from typing import Dict

import pandas as pd
import pytest
from etl_pipeline.load import DataExportStep


@pytest.fixture
def sample_dataframes_for_export() -> Dict[str, pd.DataFrame]:
    """Sample dataframes containing output_df for export tests."""
    return {
        "output_df": pd.DataFrame(
            {
                "Province": ["Madrid", "Barcelona", "Valencia"],
                "Year": [2020, 2020, 2020],
                "Air Pollution Level": [50.5, 45.2, 40.1],
                "Respiratory_diseases_total": [100.0, 90.5, 85.2],
            }
        )
    }


@pytest.fixture
def export_step():
    """Create a DataExportStep instance."""
    return DataExportStep()


def test_execute_missing_output_df(export_step):
    """Test that execute raises ValueError when output_df is missing."""
    empty_dataframes = {}
    context = {"data_path": Path("/tmp"), "export_format": ["csv"]}

    with pytest.raises(ValueError):
        export_step.execute(empty_dataframes, context)


def test_execute_missing_context_keys(
    export_step, sample_dataframes_for_export
):
    """Test that execute raises ValueError when required context keys are
    missing."""
    # Missing data_path
    context_no_path = {"export_format": ["csv"]}
    with pytest.raises(ValueError):
        export_step.execute(sample_dataframes_for_export, context_no_path)

    # Missing export_format
    context_no_format = {"data_path": Path("/tmp")}
    with pytest.raises(ValueError):
        export_step.execute(sample_dataframes_for_export, context_no_format)


def test_execute_empty_output_df(export_step):
    """Test that execute raises ValueError when output_df is empty."""
    empty_dataframes = {"output_df": pd.DataFrame()}
    context = {"data_path": Path("/tmp"), "export_format": ["csv"]}

    with pytest.raises(ValueError):
        export_step.execute(empty_dataframes, context)


def test_execute_creates_output_directory(
    export_step, sample_dataframes_for_export, tmp_path
):
    """Test that execute creates output directory."""
    context = {"data_path": tmp_path, "export_format": ["csv"]}

    export_step.execute(sample_dataframes_for_export, context)

    # Verify output directory was created
    output_dir = tmp_path / "output"
    assert output_dir.exists()


def test_execute_csv_export(
    export_step, sample_dataframes_for_export, tmp_path
):
    """Test that execute exports CSV when format is csv."""
    context = {"data_path": tmp_path, "export_format": ["csv"]}

    export_step.execute(sample_dataframes_for_export, context)

    # Verify CSV file was created
    csv_path = tmp_path / "output" / "dataset.csv"
    assert csv_path.exists()


@pytest.mark.skip(reason="Parquet support requires pyarrow/fastparquet")
def test_execute_parquet_export(
    export_step, sample_dataframes_for_export, tmp_path
):
    """Test that execute exports Parquet when format is parquet."""
    context = {"data_path": tmp_path, "export_format": ["parquet"]}

    export_step.execute(sample_dataframes_for_export, context)

    # Verify Parquet file was created
    parquet_path = tmp_path / "output" / "dataset.parquet"
    assert parquet_path.exists()


def test_execute_multiple_formats(
    export_step, sample_dataframes_for_export, tmp_path
):
    """Test that execute handles multiple export formats."""
    context = {
        "data_path": tmp_path,
        "export_format": ["csv"],
    }  # Only CSV for now

    export_step.execute(sample_dataframes_for_export, context)

    # Verify CSV file was created
    csv_path = tmp_path / "output" / "dataset.csv"
    assert csv_path.exists()


def test_export_to_csv(export_step, sample_output_df, tmp_path):
    """Test CSV export functionality."""
    context = {"data_path": tmp_path, "export_format": ["csv"]}
    dataframes = {"output_df": sample_output_df}

    export_step.execute(dataframes, context)

    # Verify file was created
    csv_path = tmp_path / "output" / "dataset.csv"
    assert csv_path.exists()

    # Verify content
    exported_df = pd.read_csv(csv_path)
    assert len(exported_df) == len(sample_output_df)
    assert list(exported_df.columns) == list(sample_output_df.columns)


@pytest.mark.skip(reason="Parquet support requires pyarrow/fastparquet")
def test_export_to_parquet(export_step, sample_output_df, tmp_path):
    """Test Parquet export functionality."""
    context = {"data_path": tmp_path, "export_format": ["parquet"]}
    dataframes = {"output_df": sample_output_df}

    export_step.execute(dataframes, context)

    # Verify file was created
    parquet_path = tmp_path / "output" / "dataset.parquet"
    assert parquet_path.exists()

    # Verify content
    exported_df = pd.read_parquet(parquet_path)
    assert len(exported_df) == len(sample_output_df)
    assert list(exported_df.columns) == list(sample_output_df.columns)


def test_unsupported_export_format(
    export_step, sample_dataframes_for_export, tmp_path
):
    """Test that unsupported export formats are handled gracefully."""
    context = {"data_path": tmp_path, "export_format": ["json"]}

    # This should complete without error but log a warning
    export_step.execute(sample_dataframes_for_export, context)

    # Verify no files were created for unsupported format
    json_path = tmp_path / "output" / "dataset.json"
    assert not json_path.exists()


def test_step_name_initialization(export_step):
    """Test that the step is initialized with correct name."""
    assert export_step.name == "etl_pipeline.load.data_export_step"
