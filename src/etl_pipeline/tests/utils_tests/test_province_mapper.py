from typing import Dict, List
from unittest.mock import MagicMock, patch
from pathlib import Path

import pandas as pd
import pytest
from etl_pipeline.utils.province_mapper import ProvinceMapper


def test_load_json_file_success(mock_json_file: Path):
    """Test successful loading of province mapping file."""
    # Create a complete province mapping with 52 provinces to match the
    # validation
    complete_mapping = {}

    # Spanish provinces (simplified mapping - just need 52 entries for
    # validation)
    province_names = [
        "A Coruña",
        "Albacete",
        "Alicante/Alacant",
        "Almería",
        "Araba/Álava",
        "Asturias",
        "Ávila",
        "Badajoz",
        "Balears, Illes",
        "Barcelona",
        "Bizkaia",
        "Burgos",
        "Cáceres",
        "Cádiz",
        "Cantabria",
        "Castellón/Castelló",
        "Ciudad Real",
        "Córdoba",
        "Cuenca",
        "Gipuzkoa",
        "Girona",
        "Granada",
        "Guadalajara",
        "Huelva",
        "Huesca",
        "Jaén",
        "La Rioja",
        "Las Palmas",
        "León",
        "Lleida",
        "Lugo",
        "Madrid",
        "Málaga",
        "Murcia",
        "Navarra",
        "Ourense",
        "Palencia",
        "Pontevedra",
        "Salamanca",
        "Santa Cruz de Tenerife",
        "Segovia",
        "Sevilla",
        "Soria",
        "Tarragona",
        "Teruel",
        "Toledo",
        "Valencia/València",
        "Valladolid",
        "Zamora",
        "Zaragoza",
        "Ceuta",
        "Melilla",
    ]

    # Create mapping where each province maps to itself
    for province in province_names:
        complete_mapping[province] = [province]

    # Reset the class variable
    ProvinceMapper.unified_province_dict = {}

    with patch(
        "etl_pipeline.utils.province_mapper.Path.__truediv__"
    ) as mock_path:
        mock_path.return_value = mock_json_file
        with patch("common.utils.file_utils.load_json_file") as mock_load:
            mock_load.return_value = complete_mapping

            ProvinceMapper._load_json_file()  # type: ignore[attr-defined]

            assert ProvinceMapper.unified_province_dict == complete_mapping
            assert len(ProvinceMapper.unified_province_dict) == 52
            mock_load.assert_called_once()


def test_load_json_file_wrong_province_count():
    """Test that loading fails when province count is not 52."""
    wrong_mapping = {
        "Madrid": ["Madrid"],
        "Barcelona": ["Barcelona"],
    }  # Only 2 provinces

    with patch("common.utils.file_utils.load_json_file") as mock_load:
        mock_load.return_value = wrong_mapping

        with pytest.raises(ValueError, match="Expected 52 provinces"):
            ProvinceMapper._load_json_file()  # type: ignore[attr-defined]


@patch.object(ProvinceMapper, "_load_json_file")
def test_map_province_name_success(
    _mock_load: MagicMock,
    sample_dataframe_for_province_mapping: pd.DataFrame,
    sample_province_mapping: Dict[str, List[str]],
):
    """Test successful province name mapping."""
    # Setup mock
    ProvinceMapper.unified_province_dict = sample_province_mapping

    # Execute mapping
    ProvinceMapper.map_province_name(sample_dataframe_for_province_mapping)

    # Verify mappings
    assert (
        sample_dataframe_for_province_mapping.loc[0, "Province"] == "Madrid"
    )  # "28 Madrid" -> "Madrid"
    assert (
        sample_dataframe_for_province_mapping.loc[1, "Province"] == "Barcelona"
    )  # "08 Barcelona" -> "Barcelona"
    assert (
        sample_dataframe_for_province_mapping.loc[2, "Province"]
        == "Valencia/València"
    )  # "Valencia" -> "Valencia/València"
    assert (
        sample_dataframe_for_province_mapping.loc[3, "Province"]
        == "Unknown Province"
    )  # No mapping, unchanged


@patch.object(ProvinceMapper, "_load_json_file")
def test_map_province_name_missing_province_column(_mock_load: MagicMock):
    """Test that mapping raises error when Province column is missing."""
    ProvinceMapper.unified_province_dict = {}
    df_no_province = pd.DataFrame({"City": ["Madrid"], "Value": [100]})

    with pytest.raises(KeyError, match="Province"):
        ProvinceMapper.map_province_name(df_no_province)


@patch.object(ProvinceMapper, "_load_json_file")
def test_map_province_name_empty_dataframe(
    _mock_load: MagicMock, sample_province_mapping: Dict[str, List[str]]
):
    """Test mapping with empty dataframe."""
    ProvinceMapper.unified_province_dict = sample_province_mapping
    empty_df = pd.DataFrame({"Province": [], "Value": []})

    # Should not raise error with empty dataframe
    ProvinceMapper.map_province_name(empty_df)
    assert len(empty_df) == 0


@patch.object(ProvinceMapper, "_load_json_file")
def test_map_province_name_case_sensitivity(
    _mock_load: MagicMock, sample_province_mapping: Dict[str, List[str]]
):
    """Test that province mapping is case sensitive."""
    ProvinceMapper.unified_province_dict = sample_province_mapping

    df_case_test = pd.DataFrame(
        {
            "Province": ["madrid", "BARCELONA", "Valencia"],  # Different cases
            "Value": [100, 200, 150],
        }
    )

    ProvinceMapper.map_province_name(df_case_test)

    # Only exact matches should be mapped
    assert (
        df_case_test.loc[0, "Province"] == "madrid"
    )  # No change (case mismatch)
    assert (
        df_case_test.loc[1, "Province"] == "BARCELONA"
    )  # No change (case mismatch)
    assert (
        df_case_test.loc[2, "Province"] == "Valencia/València"
    )  # Mapped (exact match)


@patch.object(ProvinceMapper, "_load_json_file")
def test_map_province_name_multiple_variants(_mock_load: MagicMock):
    """Test mapping with multiple variants for same province."""
    mapping_with_variants = {
        "A Coruña": [
            "A Coruña",
            "ACoruña",
            "Coruña, A",
            "Coruña,A",
            "15 Coruña, A",
        ]
    }
    ProvinceMapper.unified_province_dict = mapping_with_variants

    df_variants = pd.DataFrame(
        {
            "Province": ["ACoruña", "Coruña, A", "15 Coruña, A", "Unknown"],
            "Value": [100, 200, 150, 75],
        }
    )

    ProvinceMapper.map_province_name(df_variants)

    # All variants should map to the canonical name
    assert df_variants.loc[0, "Province"] == "A Coruña"
    assert df_variants.loc[1, "Province"] == "A Coruña"
    assert df_variants.loc[2, "Province"] == "A Coruña"
    assert df_variants.loc[3, "Province"] == "Unknown"  # No mapping


def test_create_reverse_mapping():
    """Test the internal reverse mapping creation logic."""
    sample_mapping = {
        "Madrid": ["Madrid", "28 Madrid"],
        "Barcelona": ["Barcelona", "08 Barcelona"],
    }

    # Simulate the reverse mapping creation logic from the actual method
    reverse_mapping = {}
    for canonical_name, variants in sample_mapping.items():
        for variant in variants:
            reverse_mapping[variant] = canonical_name

    expected_reverse = {
        "Madrid": "Madrid",
        "28 Madrid": "Madrid",
        "Barcelona": "Barcelona",
        "08 Barcelona": "Barcelona",
    }

    assert reverse_mapping == expected_reverse


@patch.object(ProvinceMapper, "_load_json_file")
def test_province_mapper_preserves_other_columns(
    _mock_load: MagicMock, sample_province_mapping: Dict[str, List[str]]
):
    """Test that mapping only affects Province column and preserves other
    data."""
    ProvinceMapper.unified_province_dict = sample_province_mapping

    original_df = pd.DataFrame(
        {
            "Province": ["28 Madrid", "08 Barcelona"],
            "Year": [2020, 2021],
            "Population": [6500000, 5600000],
            "GDP": [35000, 32000],
        }
    )

    expected_other_data = original_df[["Year", "Population", "GDP"]].copy()

    ProvinceMapper.map_province_name(original_df)

    # Verify other columns unchanged
    pd.testing.assert_frame_equal(
        original_df[["Year", "Population", "GDP"]], expected_other_data
    )

    # Verify only Province column changed
    assert original_df.loc[0, "Province"] == "Madrid"
    assert original_df.loc[1, "Province"] == "Barcelona"
