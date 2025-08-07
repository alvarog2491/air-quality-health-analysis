from pathlib import Path
from typing import Any, Dict

import pandas as pd
from etl_pipeline.extract import DataExtractionStep
from etl_pipeline.tests.conftest import initialize_test_data


def test_real_data_extraction_step(
    tmp_path: Path, mock_context: Dict[str, Any]
):
    """
    Tests that DataExtractionStep correctly runs all real extractors
    after setting up the expected folder and file structure.
    """

    # Prepare directory structure and input files
    initialize_test_data(tmp_path)

    # Execute extraction step
    dataframes: Dict[str, pd.DataFrame] = {}
    step = DataExtractionStep()
    step.execute(dataframes, {"data_path": tmp_path})

    # Assertions
    assert "air_quality" in dataframes
    assert "respiratory_diseases" in dataframes
    assert "life_expectancy" in dataframes
    assert "gdp" in dataframes
    assert "province_population" in dataframes

    for key in (
        "air_quality",
        "respiratory_diseases",
        "life_expectancy",
        "gdp",
        "province_population",
    ):
        df = dataframes[key]
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
