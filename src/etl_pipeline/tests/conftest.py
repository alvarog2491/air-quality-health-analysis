"""
Shared test fixtures for ETL pipeline tests.

This module contains commonly used fixtures to reduce code duplication
across test files in the ETL pipeline.
"""

from pathlib import Path
from typing import Dict, Any, List
import tempfile
import json

import pandas as pd
import pytest


@pytest.fixture
def sample_output_df() -> pd.DataFrame:
    """Sample output dataframe used across multiple test modules."""
    return pd.DataFrame(
        {
            "Province": [
                "Madrid",
                "Barcelona",
                "Las Palmas",
                "Madrid",
                "Valencia",
            ],
            "Year": [2018, 2019, 2020, 2018, 2021],
            "Air Pollution Level": [50.5, None, 45.2, 50.5, 30.1],
            "Respiratory_diseases_total": [100.0, 90.5, None, 100.0, 80.2],
            "Life_expectancy_total": ["82.1", "83.5", "81.0", "82.1", "84.2"],
            "pib": ["35000", "32000", "28000", "35000", "30000"],
        }
    )


@pytest.fixture
def sample_dataframes() -> Dict[str, pd.DataFrame]:
    """Sample dataframes for testing merging operations."""
    return {
        "air_quality": pd.DataFrame(
            {
                "Province": ["Madrid", "Barcelona", "Valencia"],
                "Year": [2020, 2020, 2020],
                "Air Pollution Level": [50.5, 45.2, 40.1],
            }
        ),
        "respiratory_diseases": pd.DataFrame(
            {
                "Province": ["Madrid", "Barcelona", "Valencia"],
                "Year": [2020, 2020, 2020],
                "Respiratory_diseases_total": [100.0, 90.5, 85.2],
            }
        ),
        "life_expectancy": pd.DataFrame(
            {
                "Province": ["Madrid", "Barcelona", "Valencia"],
                "Year": [2020, 2020, 2020],
                "Life_expectancy_total": [82.1, 83.5, 81.8],
            }
        ),
        "gdp": pd.DataFrame(
            {
                "Province": ["Madrid", "Barcelona", "Valencia"],
                "Year": [2020, 2020, 2020],
                "pib": [35000.0, 32000.0, 28000.0],
            }
        ),
        "province_population": pd.DataFrame(
            {
                "Province": ["Madrid", "Barcelona", "Valencia", "Madrid", "Barcelona", "Valencia"],
                "Year": [2019, 2019, 2019, 2020, 2020, 2020],
                "Population": [6400000, 5500000, 2450000, 6500000, 5600000, 2500000],
            }
        ),
    }


@pytest.fixture
def sample_dataframes_for_transformation() -> Dict[str, pd.DataFrame]:
    """Sample dataframes for testing transformation operations with
    different column names."""
    return {
        "air_quality": pd.DataFrame(
            {
                "Province": ["Madrid", "Barcelona"],
                "Year": [2020, 2021],
                "Air Pollution Level": [50.5, 45.2],
            }
        ),
        "respiratory_diseases": pd.DataFrame(
            {
                "Province": ["Madrid", "Barcelona"],
                "Year": [2020, 2021],
                "Deaths": [100, 90],
            }
        ),
        "life_expectancy": pd.DataFrame(
            {
                "Province": ["Madrid", "Barcelona"],
                "Year": [2020, 2021],
                "Life_Expectancy": [82.1, 83.5],
            }
        ),
        "gdp": pd.DataFrame(
            {
                "Province": ["Madrid", "Barcelona"],
                "Year": [2020, 2021],
                "GDP": [35000, 32000],
            }
        ),
        "province_population": pd.DataFrame(
            {
                "Province": ["Madrid", "Barcelona", "Madrid", "Barcelona"],
                "Year": [2019, 2020, 2021, 2022],
                "Population": [6400000, 5500000, 6600000, 5700000],
            }
        ),
    }


@pytest.fixture
def dataframes_with_output(
    sample_output_df: pd.DataFrame,
) -> Dict[str, pd.DataFrame]:
    """Sample dataframes containing output_df for cleaning and export tests."""
    return {"output_df": sample_output_df}


@pytest.fixture
def sample_dataframe_for_province_mapping():
    """Sample dataframe with province names to normalize."""
    return pd.DataFrame(
        {
            "Province": [
                "28 Madrid",
                "08 Barcelona",
                "Valencia",
                "Unknown Province",
            ],
            "Year": [2020, 2020, 2020, 2020],
            "Value": [100, 200, 150, 75],
        }
    )


@pytest.fixture
def sample_province_mapping() -> Dict[str, List[str]]:
    """Sample province mapping for testing."""
    return {
        "Madrid": ["Madrid", "28 Madrid", "Comunidad de Madrid"],
        "Barcelona": ["Barcelona", "08 Barcelona", "Barna"],
        "Valencia/València": ["Valencia", "46 Valencia/València", "València"],
    }


@pytest.fixture
def mock_json_file(sample_province_mapping: Dict[str, List[str]]):
    """Create a temporary JSON file with province mapping."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as f:
        json.dump(sample_province_mapping, f)
        return Path(f.name)


@pytest.fixture
def mock_context(tmp_path: Path) -> Dict[str, Any]:
    """Mock context for test execution."""
    return {"data_path": str(tmp_path)}


def initialize_test_data(tmp_path: Path) -> None:
    """
    Initializes the full directory structure and creates realistic CSV files
    required by air quality, health, and socioeconomic extractors.
    """

    # Air quality data
    air_path = tmp_path / "air_quality_data" / "raw"
    air_path.mkdir(parents=True)
    air_file = air_path / "air_quality_with_province.csv"
    pd.DataFrame(
        {
            "Country": ["Spain"],
            "Air Quality Network": ["NET_ES131A"],
            "Air Quality Network Name": ["Ayto Madrid"],
            "Air Quality Station EoI Code": ["ES0125A"],
            "Air Quality Station Name": ["VILLAVERDE"],
            "Sampling Point Id": ["SP_28079017_8_8"],
            "Air Pollutant": ["NO2"],
            "Air Pollutant Description": ["Nitrogen dioxide (air)"],
            "Data Aggregation Process Id": ["P1Y"],
            "Data Aggregation Process": ["Annual mean / 1 calendar year"],
            "Year": ["1991"],
            "Air Pollution Level": [80.639],
            "Unit Of Air Pollution Level": ["ug/m3"],
            "Data Coverage": [94.77],
            "Verification": [None],
            "Air Quality Station Type": ["Background"],
            "Air Quality Station Area": ["urban"],
            "Longitude": [-3.705],
            "Latitude": [40.3469],
            "Altitude": [593.0],
            "City": ["Madrid"],
            "City Code": ["ES001K1"],
            "City Population": [5098717.0],
            "Source Of Data Flow": ["Reporting within EoI/AirBase 8"],
            "Calculation Time": ["31/12/2012 00:00:00"],
            "Link to raw data (only E1a/validated data from AQ "
            "e-Reporting)": [None],
            "Observation Frequency": [None],
            "Province": ["Madrid"],
        }
    ).to_csv(air_file, index=False)

    # Health data
    health_path = tmp_path / "health_data" / "raw"
    health_path.mkdir(parents=True)

    # Enfermedades respiratorias
    (health_path / "enfermedades_respiratorias.csv").write_text(
        "Causa de muerte;Sexo;Provincias;Periodo;Total\n"
        "062-067  X.Enfermedades del sistema respiratorio;Total;02 "
        "Albacete;2023;397\n"
    )

    # Esperanza de vida
    (health_path / "esperanza_vida.csv").write_text(
        "Sexo;Provincias;Periodo;Total\n"
        "Ambos sexos;02 Albacete;2023;83,61\n"
    )

    # Socioeconomic data
    socio_path = tmp_path / "socioeconomic_data" / "raw"
    socio_path.mkdir(parents=True)

    # PIB per capita
    (socio_path / "PIB per cap provincias 2000-2021.csv").write_text(
        "Provincia,Valor\n02 Albacete,21000\n"
    )

    # Población provincias 2021
    (socio_path / "poblacion_provincias_21.csv").write_text(
        "Provincias;Sexo;Periodo;Total\n" "02 Albacete;Total;2021;386.464\n"
    )

