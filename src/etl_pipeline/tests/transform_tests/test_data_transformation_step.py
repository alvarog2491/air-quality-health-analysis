from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from etl_pipeline.transform import DataTransformationStep


@pytest.fixture
def transformation_step():
    """Create a DataTransformationStep instance."""
    return DataTransformationStep()


@patch(
    "etl_pipeline.transform.data_transformation_step."
    "AirQualityDataTransformer"
)
@patch("etl_pipeline.transform.data_transformation_step.HealthDataTransformer")
@patch(
    "etl_pipeline.transform.data_transformation_step."
    "SocioeconomicDataTransformer"
)
def test_execute_transforms_all_data(
    mock_socio_transformer,
    mock_health_transformer,
    mock_air_transformer,
    transformation_step,
    sample_dataframes_for_transformation,
):
    """Test that execute method calls all transformers correctly."""
    # Setup mocks
    mock_air_instance = MagicMock()
    mock_air_instance.transform.return_value = (
        sample_dataframes_for_transformation["air_quality"],
    )
    mock_air_transformer.return_value = mock_air_instance

    mock_health_instance = MagicMock()
    mock_health_instance.transform.return_value = (
        sample_dataframes_for_transformation["respiratory_diseases"],
        sample_dataframes_for_transformation["life_expectancy"],
    )
    mock_health_transformer.return_value = mock_health_instance

    mock_socio_instance = MagicMock()
    mock_socio_instance.transform.return_value = (
        sample_dataframes_for_transformation["gdp"],
        sample_dataframes_for_transformation["province_population"],
    )
    mock_socio_transformer.return_value = mock_socio_instance

    # Execute
    context = {}
    transformation_step.execute(sample_dataframes_for_transformation, context)

    # Verify transformers were called
    mock_air_instance.transform.assert_called_once_with(
        sample_dataframes_for_transformation["air_quality"]
    )
    mock_health_instance.transform.assert_called_once_with(
        sample_dataframes_for_transformation["respiratory_diseases"],
        sample_dataframes_for_transformation["life_expectancy"],
    )
    mock_socio_instance.transform.assert_called_once_with(
        sample_dataframes_for_transformation["gdp"],
        sample_dataframes_for_transformation["province_population"],
    )


def test_step_name_initialization(transformation_step):
    """Test that the step is initialized with correct name."""
    assert transformation_step.name == "Data Transformation"


def test_execute_missing_dataframe():
    """Test that execute handles missing dataframes gracefully."""
    step = DataTransformationStep()
    incomplete_dataframes = {"air_quality": pd.DataFrame()}

    with pytest.raises(ValueError, match="Input DataFrame is empty"):
        step.execute(incomplete_dataframes, {})
