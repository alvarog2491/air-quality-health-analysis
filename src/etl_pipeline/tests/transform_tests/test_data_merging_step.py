from typing import Dict
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from etl_pipeline.transform import DataMergingStep


@pytest.fixture
def merging_step() -> DataMergingStep:
    """Create a DataMergingStep instance."""
    return DataMergingStep()


def test_execute_successful_merge(merging_step: DataMergingStep, sample_dataframes: Dict[str, pd.DataFrame]):
    """Test successful execution of data merging."""
    context = {}

    # Execute
    merging_step.execute(sample_dataframes, context)

    # Verify output_df was created
    assert "output_df" in sample_dataframes
    merged_df = sample_dataframes["output_df"]

    # Verify merge results
    assert len(merged_df) == 3
    assert "Province" in merged_df.columns
    assert "Year" in merged_df.columns
    assert "Air Pollution Level" in merged_df.columns
    assert "Respiratory_diseases_total" in merged_df.columns
    assert "Life_expectancy_total" in merged_df.columns
    assert "pib" in merged_df.columns


def test_execute_missing_dataframe(merging_step: DataMergingStep):
    """Test that execute handles missing dataframes."""
    incomplete_dataframes = {
        "air_quality": pd.DataFrame(),
        "respiratory_diseases": pd.DataFrame(),
        # Missing other required dataframes
    }

    with pytest.raises(KeyError):
        merging_step.execute(incomplete_dataframes, {})


@patch.object(DataMergingStep, "_validate_merge_columns")
@patch.object(DataMergingStep, "_merge_all_data")
def test_execute_calls_validation_and_merge(
    mock_merge_all: MagicMock, mock_validate: MagicMock, merging_step: DataMergingStep, sample_dataframes: Dict[str, pd.DataFrame]
):
    """Test that execute calls validation and merge methods."""
    # Setup mock return value
    mock_merge_all.return_value = pd.DataFrame({"test": [1, 2, 3]})

    # Execute
    merging_step.execute(sample_dataframes, {})

    # Verify methods were called
    mock_validate.assert_called_once()
    mock_merge_all.assert_called_once()


def test_validate_merge_columns_success(merging_step: DataMergingStep, sample_dataframes: Dict[str, pd.DataFrame]):
    """Test that validation passes with correct merge columns."""
    # This should not raise an exception
    merging_step._validate_merge_columns(  # type: ignore[attr-defined]
        sample_dataframes["air_quality"],
        sample_dataframes["respiratory_diseases"],
        sample_dataframes["life_expectancy"],
        sample_dataframes["gdp"],
        sample_dataframes["province_population"],
    )


def test_validate_merge_columns_missing_column(merging_step: DataMergingStep):
    """Test that validation fails with missing merge columns."""
    # Create dataframes missing required columns
    df_missing_province = pd.DataFrame({"Year": [2020], "Value": [100]})
    df_with_columns = pd.DataFrame({"Province": ["Madrid"], "Year": [2020]})

    with pytest.raises(ValueError, match="DataFrame missing columns"):
        merging_step._validate_merge_columns(  # type: ignore[attr-defined]
            df_missing_province,
            df_with_columns,
            df_with_columns,
            df_with_columns,
            df_with_columns,
        )


def test_merge_all_data(merging_step: DataMergingStep, sample_dataframes: Dict[str, pd.DataFrame]):
    """Test the actual merging logic."""
    merged_df = merging_step._merge_all_data(  # type: ignore[attr-defined]
        sample_dataframes["air_quality"],
        sample_dataframes["respiratory_diseases"],
        sample_dataframes["life_expectancy"],
        sample_dataframes["gdp"],
        sample_dataframes["province_population"],
    )

    # Verify merge results
    assert len(merged_df) == 3
    assert (
        len(merged_df.columns) > 5
    )  # Should have columns from all dataframes

    # Verify specific data is present
    madrid_row = merged_df[merged_df["Province"] == "Madrid"].iloc[0]
    assert madrid_row["Air Pollution Level"] == 50.5
    assert madrid_row["Respiratory_diseases_total"] == 100.0
    assert madrid_row["Life_expectancy_total"] == 82.1
    assert madrid_row["pib"] == 35000.0


def test_merge_with_missing_data():
    """Test merging when some dataframes have missing provinces."""
    merging_step = DataMergingStep()

    air_quality = pd.DataFrame(
        {
            "Province": ["Madrid", "Barcelona"],
            "Year": [2020, 2020],
            "Air Pollution Level": [50.5, 45.2],
        }
    )

    respiratory = pd.DataFrame(
        {
            "Province": ["Madrid"],  # Barcelona missing
            "Year": [2020],
            "Respiratory_diseases_total": [100.0],
        }
    )

    life_expectancy = pd.DataFrame(
        {
            "Province": ["Madrid", "Barcelona"],
            "Year": [2020, 2020],
            "Life_expectancy_total": [82.1, 83.5],
        }
    )

    gdp = pd.DataFrame(
        {
            "Province": ["Madrid", "Barcelona"],
            "Year": [2020, 2020],
            "pib": [35000.0, 32000.0],
        }
    )

    population = pd.DataFrame(
        {
            "Province": ["Madrid", "Barcelona"],
            "Year": [2020, 2020],
            "Population": [6500000, 5600000],
        }
    )

    merged_df = merging_step._merge_all_data(  # type: ignore[attr-defined]
        air_quality, respiratory, life_expectancy, gdp, population
    )

    # Should have 2 rows (left join from air_quality)
    assert len(merged_df) == 2

    # Barcelona should have NaN for respiratory diseases
    barcelona_row = merged_df[merged_df["Province"] == "Barcelona"].iloc[0]
    assert pd.isna(barcelona_row["Respiratory_diseases_total"])


def test_step_name_initialization(merging_step: DataMergingStep):
    """Test that the step is initialized with correct name."""
    assert merging_step.name == "etl_pipeline.transform.data_merging_step"
