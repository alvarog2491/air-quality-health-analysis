import json
import shutil
from pathlib import Path
from typing import Any, Dict, Union
import logging

import joblib
import yaml

logger = logging.getLogger("FileUtils")


def load_yaml_config(
    yaml_path: Union[str, Path],
) -> Dict[str, Any]:
    """
    Load a YAML configuration file from the specified path.

    Args:
        yaml_path (str | Path): Path to the YAML configuration file.

    Returns:
        Dict[str, Union[str, List[str], Dict[str, Any]]]: Parsed YAML content
            as a dictionary.

    Raises:
        FileNotFoundError: If the file does not exist.
        yaml.YAMLError: If YAML parsing fails.
    """
    path = Path(yaml_path)
    if not path.is_file():
        raise FileNotFoundError(f"Expected file not found: {yaml_path}")

    with path.open("r", encoding="utf-8") as fp:
        return yaml.safe_load(fp)


def load_json_file(json_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Load and parse a JSON file from the specified path.

    Args:
        json_path (str | Path): Path to the JSON file.

    Returns:
        Dict[str, Any]: Parsed JSON content as a dictionary.

    Raises:
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file content is not valid JSON.
    """
    path = Path(json_path)
    if not path.is_file():
        raise FileNotFoundError(f"Expected file not found: {json_path}")

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def reset_directory(path: str) -> None:
    """
    Delete the directory at the given path if it exists, then recreate it.

    Parameters:
    path (str): Path to the directory to reset.
    """
    try:
        shutil.rmtree(path)
    except Exception:
        pass
    finally:
        Path(path).mkdir(parents=True, exist_ok=True)


def create_directory(path: str) -> None:
    """
    Create a directory at the specified path if it does not already exist.

    Parameters:
    path (str): Path to the directory to create.
    """
    Path(path).mkdir(parents=True, exist_ok=True)


def load_pickle_file(file_path: str) -> Any:
    """
    Load a pickle file from the specified path.

    Args:
        file_path (Path): Path to the pickle file.

    Returns:
        Any: The loaded data.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Expected file not found: {file_path}")

    return joblib.load(file_path)


def save_pickle_file(data: Any, file_path: str) -> None:
    """
    Save data to a pickle file at the specified path.

    Args:
        data (Any): Data to save.
        file_path (Path): Path where the data should be saved.
    """
    create_directory(file_path)
    joblib.dump(data, file_path)


class ValidationError(Exception):
    """Custom exception for validation errors."""

    pass


def validate_file_exists(file_path: str, description: str = "File") -> Path:
    """
    Validate that a file exists.

    Args:
        file_path: Path to the file
        description: Description of the file for error messages

    Returns:
        Path object if file exists

    Raises:
        ValidationError: If file doesn't exist
    """
    path = Path(file_path)
    if not path.exists():
        raise ValidationError(f"{description} not found at: {file_path}")
    if not path.is_file():
        raise ValidationError(f"{description} path is not a file: {file_path}")
    return path


def validate_directory_exists(
    dir_path: str, create_if_missing: bool = False
) -> Path:
    """
    Validate that a directory exists, optionally creating it.

    Args:
        dir_path: Path to the directory
        create_if_missing: Whether to create directory if it doesn't exist

    Returns:
        Path object

    Raises:
        ValidationError: If directory doesn't exist and
            create_if_missing is False
    """
    path = Path(dir_path)
    if not path.exists():
        if create_if_missing:
            path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {path}")
        else:
            raise ValidationError(f"Directory not found: {dir_path}")
    elif not path.is_dir():
        raise ValidationError(f"Path is not a directory: {dir_path}")
    return path
