import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from etl_pipeline.etl_step import ETLStep
from etl_pipeline.main_orchestrator import ETLPipeline


class MockETLStep(ETLStep):
    """Mock ETL step for testing."""

    def __init__(self, name: str = "MockStep"):
        super().__init__(name)
        self.executed = False
        self.execution_order = None
        self.execute_mock = MagicMock()

    def execute(
        self, dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]
    ) -> None:
        """Mock execute method."""
        self.executed = True
        self.execute_mock(dataframes, context)
        # Add mock data for testing
        if "output_df" not in dataframes:
            dataframes["output_df"] = pd.DataFrame({"test": [1, 2, 3]})


@pytest.fixture
def mock_etl_steps() -> List[MockETLStep]:
    """Create mock ETL steps for testing."""
    return [MockETLStep("Step1"), MockETLStep("Step2"), MockETLStep("Step3")]


@pytest.fixture
def pipeline_with_mock_steps(mock_etl_steps: List[MockETLStep]) -> ETLPipeline:
    """Create ETL pipeline with mock steps."""
    return ETLPipeline(steps=mock_etl_steps)


@pytest.fixture
def temp_data_path() -> Path:
    """Create temporary data path for testing."""
    temp_dir = Path(tempfile.mkdtemp())
    return temp_dir


def test_pipeline_initialization_with_custom_steps(
    mock_etl_steps: List[MockETLStep],
):
    """Test ETL pipeline initialization with custom steps."""
    pipeline = ETLPipeline(steps=mock_etl_steps)

    assert pipeline.steps == mock_etl_steps
    assert len(pipeline.steps) == 3
    assert pipeline.logger is not None


@patch("etl_pipeline.main_orchestrator.ETLPipeline._get_default_steps")
def test_pipeline_initialization_with_default_steps(
    mock_get_default: MagicMock,
):
    """Test ETL pipeline initialization with default steps."""
    mock_steps = [MockETLStep("DefaultStep")]
    mock_get_default.return_value = mock_steps

    pipeline = ETLPipeline()

    mock_get_default.assert_called_once()
    assert pipeline.steps == mock_steps


def test_get_default_steps():
    """Test that default steps are created correctly."""
    pipeline = ETLPipeline()
    # Access via private attribute instead of protected method to avoid Pylance error
    default_steps = pipeline._get_default_steps()  # type: ignore[attr-defined]

    # Verify we have the expected number of steps
    assert len(default_steps) == 8

    # Verify the step types and order
    step_types = [type(step).__name__ for step in default_steps]
    expected_types = [
        "DataExtractionStep",
        "DataTransformationStep",
        "DataMergingStep",
        "FeatureEngineeringStep",
        "DataCleaningStep",
        "DataValidationStep",
        "DataExportStep",
        "DataQualityReportStep",
    ]

    assert step_types == expected_types


@patch("etl_pipeline.main_orchestrator.CheckProjectStructure")
def test_run_executes_all_steps(
    mock_check_structure: MagicMock, 
    pipeline_with_mock_steps: ETLPipeline, 
    temp_data_path: Path
):
    """Test that run method executes all steps in order."""
    # Setup mocks
    mock_check_instance = MagicMock()
    mock_check_instance.execute.return_value = temp_data_path
    mock_check_structure.return_value = mock_check_instance

    # Mock context data that run() expects
    mock_context_data: Dict[str, Any] = {
        "output_file_path": str(temp_data_path / "output.csv"),
        "output_file": pd.DataFrame({"test": [1, 2, 3]}),
        "reports_path": str(temp_data_path / "reports"),
    }

    # Setup the last step to add required context data
    def last_step_execute(dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]) -> None:
        context.update(mock_context_data)
        if "output_df" not in dataframes:
            dataframes["output_df"] = pd.DataFrame({"test": [1, 2, 3]})

    pipeline_with_mock_steps.steps[-1].execute = MagicMock(
        side_effect=last_step_execute
    )

    # Execute
    result_df, metadata = pipeline_with_mock_steps.run()

    # Verify all steps were executed
    for step in pipeline_with_mock_steps.steps[:-1]:  # All but last
        assert step.executed  # type: ignore[attr-defined]

    # Verify last step was called
    assert pipeline_with_mock_steps.steps[-1].execute.called

    # Verify return values
    assert isinstance(result_df, pd.DataFrame)
    assert isinstance(metadata, dict)
    assert "execution_time" in metadata
    assert isinstance(metadata["execution_time"], timedelta)


@patch("etl_pipeline.main_orchestrator.CheckProjectStructure")
def test_run_creates_correct_context(
    mock_check_structure: MagicMock, 
    pipeline_with_mock_steps: ETLPipeline, 
    temp_data_path: Path
):
    """Test that run method creates correct context."""
    # Setup mocks
    mock_check_instance = MagicMock()
    mock_check_instance.execute.return_value = temp_data_path
    mock_check_structure.return_value = mock_check_instance

    # Track context passed to steps
    context_calls: List[Dict[str, Any]] = []

    def track_context(_dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]) -> None:
        context_calls.append(context.copy())
        # Add required context for run method
        context.update(
            {
                "output_file_path": str(temp_data_path / "output.csv"),
                "output_file": pd.DataFrame({"test": [1, 2, 3]}),
                "reports_path": str(temp_data_path / "reports"),
            }
        )

    # Mock all steps to track context
    for step in pipeline_with_mock_steps.steps:
        step.execute = MagicMock(side_effect=track_context)

    # Execute
    pipeline_with_mock_steps.run()

    # Verify context structure
    first_context: Dict[str, Any] = context_calls[0]
    assert "data_path" in first_context
    assert "export_format" in first_context
    assert first_context["data_path"] == temp_data_path
    assert first_context["export_format"] == ["csv"]


@patch("etl_pipeline.main_orchestrator.CheckProjectStructure")
def test_run_measures_execution_time(
    mock_check_structure: MagicMock, 
    pipeline_with_mock_steps: ETLPipeline, 
    temp_data_path: Path
):
    """Test that run method measures execution time correctly."""
    # Setup mocks
    mock_check_instance = MagicMock()
    mock_check_instance.execute.return_value = temp_data_path
    mock_check_structure.return_value = mock_check_instance

    # Mock slow execution
    def slow_execute(_dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]) -> None:
        import time

        time.sleep(0.01)  # Small delay for testing
        context.update(
            {
                "output_file_path": str(temp_data_path / "output.csv"),
                "output_file": pd.DataFrame({"test": [1, 2, 3]}),
                "reports_path": str(temp_data_path / "reports"),
            }
        )

    # Only mock the last step to add required context
    pipeline_with_mock_steps.steps[-1].execute = MagicMock(
        side_effect=slow_execute
    )

    # Execute
    _result_df, metadata = pipeline_with_mock_steps.run()

    # Verify execution time is measured
    assert isinstance(metadata["execution_time"], timedelta)
    assert metadata["execution_time"].total_seconds() > 0


@patch("etl_pipeline.main_orchestrator.CheckProjectStructure")
def test_run_handles_step_failure(
    mock_check_structure: MagicMock, 
    pipeline_with_mock_steps: ETLPipeline, 
    temp_data_path: Path
):
    """Test that run method handles step failures properly."""
    # Setup mocks
    mock_check_instance = MagicMock()
    mock_check_instance.execute.return_value = temp_data_path
    mock_check_structure.return_value = mock_check_instance

    # Make second step fail
    pipeline_with_mock_steps.steps[1].execute = MagicMock(
        side_effect=ValueError("Test error")
    )

    # Execute and expect exception
    with pytest.raises(ValueError, match="Test error"):
        pipeline_with_mock_steps.run()

    # Verify first step was executed but second failed
    assert pipeline_with_mock_steps.steps[0].executed  # type: ignore[attr-defined]
    assert pipeline_with_mock_steps.steps[1].execute.called
    # Third step should not have been called due to failure
    assert not pipeline_with_mock_steps.steps[2].executed  # type: ignore[attr-defined]


def test_run_empty_steps_list():
    """Test run method with empty steps list."""
    # Create pipeline and manually set empty steps (to bypass the
    # constructor's default behavior)
    pipeline = ETLPipeline()
    pipeline.steps = []  # Force empty steps list

    # Mock CheckProjectStructure to avoid file system operations
    with patch(
        "etl_pipeline.main_orchestrator.CheckProjectStructure"
    ) as mock_check_structure:
        mock_check_instance = MagicMock()
        mock_check_instance.execute.return_value = Path("/tmp")
        mock_check_structure.return_value = mock_check_instance

        # This should fail because no steps provide required context
        # The actual error will be KeyError when trying to access context keys
        with pytest.raises(KeyError):
            pipeline.run()


def test_pipeline_logger_initialization():
    """Test that pipeline logger is initialized correctly."""
    pipeline = ETLPipeline()

    assert pipeline.logger is not None
    assert pipeline.logger.name == "ETLPipeline"


@patch("etl_pipeline.main_orchestrator.CheckProjectStructure")
def test_run_dataframes_persistence(
    mock_check_structure: MagicMock, 
    pipeline_with_mock_steps: ETLPipeline, 
    temp_data_path: Path
):
    """Test that dataframes are persisted across steps."""
    # Setup mocks
    mock_check_instance = MagicMock()
    mock_check_instance.execute.return_value = temp_data_path
    mock_check_structure.return_value = mock_check_instance

    # Track dataframes passed to each step
    dataframes_calls: List[List[str]] = []

    def track_dataframes(dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]) -> None:
        dataframes_calls.append(list(dataframes.keys()))
        # First step adds a dataframe
        if len(dataframes_calls) == 1:
            dataframes["test_data"] = pd.DataFrame({"col1": [1, 2, 3]})
        # Last step adds required context
        if len(dataframes_calls) == len(pipeline_with_mock_steps.steps):
            context.update(
                {
                    "output_file_path": str(temp_data_path / "output.csv"),
                    "output_file": pd.DataFrame({"test": [1, 2, 3]}),
                    "reports_path": str(temp_data_path / "reports"),
                }
            )

    # Mock all steps
    for step in pipeline_with_mock_steps.steps:
        step.execute = MagicMock(side_effect=track_dataframes)

    # Execute
    pipeline_with_mock_steps.run()

    # Verify dataframes persist across steps
    assert len(dataframes_calls) == 3
    # First step sees empty dataframes (or minimal setup)
    # Second and third steps should see the test_data added by first step
    for i in range(1, len(dataframes_calls)):
        assert (
            "test_data" in dataframes_calls[i] or len(dataframes_calls[i]) > 0
        )


def test_setup_logger_called_on_import():
    """Test that setup_logger is called when module is imported."""
    # This test verifies the module-level setup_logger() call exists
    # We can't easily test the actual call without complex module reloading
    # Instead, verify that the setup_logger import and call exist in the module
    import inspect

    import etl_pipeline.main_orchestrator as main_orchestrator

    # Verify the module has the setup_logger import and call
    source = inspect.getsource(main_orchestrator)
    assert "from etl_pipeline.config.logger import setup_logger" in source
    assert "setup_logger()" in source
