import pandas as pd
import pytest
from etl_pipeline.load import DataQualityReportStep


@pytest.fixture
def quality_report_step():
    """Create a DataQualityReportStep instance."""
    return DataQualityReportStep()


def test_execute_missing_output_df(quality_report_step):
    """Test that execute raises ValueError when output_df is missing."""
    empty_dataframes = {}
    context = {"data_path": "/tmp"}

    with pytest.raises(ValueError):
        quality_report_step.execute(empty_dataframes, context)


def test_execute_missing_data_path(
    quality_report_step, dataframes_with_output
):
    """Test that execute raises ValueError when data_path is missing."""
    context = {}

    with pytest.raises(ValueError):
        quality_report_step.execute(dataframes_with_output, context)


def test_execute_creates_output_directory(
    quality_report_step, dataframes_with_output, tmp_path
):
    """Test that execute creates output directory and sets context paths."""
    context = {"data_path": str(tmp_path)}

    quality_report_step.execute(dataframes_with_output, context)

    # Verify output directory was created
    output_dir = tmp_path / "output" / "reports"
    assert output_dir.exists()

    # Verify context was updated
    assert "quality_report_path" in context
    assert "reports_path" in context
    assert context["quality_report_path"].endswith("data_quality_report.json")

    # Verify report file was created
    report_file = tmp_path / "output" / "reports" / "data_quality_report.json"
    assert report_file.exists()


def test_generate_report(quality_report_step, sample_output_df, tmp_path):
    """Test that report generation creates correct structure."""
    quality_report_step._output_dir = tmp_path / "output" / "reports"
    quality_report_step._report_file = (
        quality_report_step._output_dir / "data_quality_report.json"
    )

    quality_report_step._generate_report(sample_output_df)

    # Verify report file was created
    assert quality_report_step._report_file.exists()

    # Read and verify report content
    import json

    with open(quality_report_step._report_file, "r") as f:
        report_data = json.load(f)

    # Verify report structure matches actual implementation
    assert "total_records" in report_data
    assert "total_columns" in report_data
    assert "memory_usage_mb" in report_data
    assert "duplicate_rows" in report_data
    assert "missing_data" in report_data
    assert "data_types" in report_data

    # Verify missing data structure
    missing_data = report_data["missing_data"]
    assert "total_missing_values" in missing_data
    assert "columns_with_missing" in missing_data
    assert "missing_percentage" in missing_data


def test_missing_data_stats(quality_report_step):
    """Test missing data statistics calculation."""
    # Create test dataframe with known null values
    test_df = pd.DataFrame(
        {
            "Province": ["Madrid", "Barcelona", "Valencia"],
            "Year": [2020, 2020, 2020],
            "Air Pollution Level": [50.5, None, 40.1],  # 1 null
            "Respiratory_diseases_total": [100.0, 90.5, None],  # 1 null
        }
    )

    stats = quality_report_step._missing_data_stats(test_df)

    assert "total_missing_values" in stats
    assert "columns_with_missing" in stats
    assert "missing_percentage" in stats

    # Should have 2 missing values
    assert stats["total_missing_values"] == 2
    # Should have 2 columns with missing values
    assert stats["columns_with_missing"] == 2
    # Calculate expected percentage
    total_cells = len(test_df) * len(test_df.columns)
    expected_pct = (2 / total_cells) * 100
    assert abs(stats["missing_percentage"] - expected_pct) < 0.01


def test_save_report(quality_report_step, tmp_path):
    """Test saving report to file."""
    quality_report_step._output_dir = tmp_path / "output" / "reports"
    quality_report_step._report_file = (
        quality_report_step._output_dir / "data_quality_report.json"
    )

    test_report = {
        "total_records": 100,
        "total_columns": 5,
        "memory_usage_mb": 1.5,
    }

    quality_report_step._save_report(test_report)

    # Verify directory and file were created
    assert quality_report_step._output_dir.exists()
    assert quality_report_step._report_file.exists()

    # Verify report content
    import json

    with open(quality_report_step._report_file, "r") as f:
        saved_report = json.load(f)

    assert saved_report == test_report


def test_step_name_initialization(quality_report_step):
    """Test that the step is initialized with correct name."""
    assert (
        quality_report_step.name
        == "etl_pipeline.load.data_quality_report_step"
    )


def test_full_execute_integration(
    quality_report_step, dataframes_with_output, tmp_path
):
    """Test full execute method integration."""
    context = {"data_path": str(tmp_path)}

    quality_report_step.execute(dataframes_with_output, context)

    # Verify output directory was created
    output_dir = tmp_path / "output" / "reports"
    assert output_dir.exists()

    # Verify report file was created
    report_file = tmp_path / "output" / "reports" / "data_quality_report.json"
    assert report_file.exists()

    # Verify context was properly updated
    assert "quality_report_path" in context
    assert "reports_path" in context

    # Verify report has correct structure
    import json

    with open(report_file, "r") as f:
        report_data = json.load(f)

    assert "total_records" in report_data
    assert "total_columns" in report_data
    assert "memory_usage_mb" in report_data
    assert "duplicate_rows" in report_data
    assert "missing_data" in report_data
