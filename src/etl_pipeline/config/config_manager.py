"""
Configuration Manager for ETL Pipeline

Provides centralized configuration management with environment-specific
settings and validation of configuration values.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


class ConfigManager:
    """
    Centralized configuration manager for the ETL pipeline.

    Loads configuration from YAML files with support for environment-specific
    overrides and provides typed access to configuration values.
    """

    def __init__(
        self, env: Optional[str] = None, config_path: Optional[Path] = None
    ):
        """
        Initialize the configuration manager.

        Args:
            env: Environment name (development, production, testing)
            config_path: Path to configuration directory
        """
        self.env = env or os.getenv("ETL_ENV", "development")
        self.config_path = config_path or Path(__file__).parent
        self.config = self._load_config()
        self.logger = logging.getLogger(self.__class__.__name__)

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML files."""
        try:
            # Load base configuration
            base_config_file = self.config_path / "pipeline_config.yaml"
            with open(base_config_file, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            # Load environment-specific overrides if they exist
            env_config_file = (
                self.config_path / f"pipeline_config_{self.env}.yaml"
            )
            if env_config_file.exists():
                with open(env_config_file, "r", encoding="utf-8") as f:
                    env_config = yaml.safe_load(f)
                config = self._merge_configs(config, env_config)

            return config

        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration: {str(e)}")

    def _merge_configs(
        self, base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Recursively merge configuration dictionaries."""
        result: Dict[str, Any] = base.copy()
        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                base_dict: Dict[str, Any] = result[key]  # type: ignore
                override_dict: Dict[str, Any] = value  # type: ignore
                result[key] = self._merge_configs(base_dict, override_dict)
            else:
                result[key] = value
        return result

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value by dot-separated key path.

        Args:
            key_path: Dot-separated path to configuration value
                (e.g., 'data_sources.air_quality.directory')
            default: Default value if key is not found

        Returns:
            Configuration value or default
        """
        keys = key_path.split(".")
        value = self.config

        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            if default is not None:
                return default
            raise ConfigurationError(
                f"Configuration key '{key_path}' not found"
            )

    def get_data_source_config(self, source: str) -> Dict[str, Any]:
        """Get configuration for a specific data source."""
        return self.get(f"data_sources.{source}", {})

    def get_processing_config(self) -> Dict[str, Any]:
        """Get processing configuration."""
        return self.get("processing", {})

    def get_validation_config(self) -> Dict[str, Any]:
        """Get validation configuration."""
        return self.get("validation", {})

    def get_output_config(self) -> Dict[str, Any]:
        """Get output configuration."""
        return self.get("output", {})

    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        return self.get("logging", {})

    def get_pipeline_steps(self) -> List[Dict[str, Any]]:
        """Get enabled pipeline steps configuration."""
        steps = self.get("pipeline.steps", [])
        return [step for step in steps if step.get("enabled", True)]

    def get_file_path(
        self, data_source: str, file_type: str, base_path: Path
    ) -> Path:
        """
        Get full file path for a data source file.

        Args:
            data_source: Data source name (air_quality, health, socioeconomic)
            file_type: File type (raw_file, processed_file, etc.)
            base_path: Base data directory path

        Returns:
            Full path to the file
        """
        source_config = self.get_data_source_config(data_source)
        directory = source_config.get("directory", "")
        filename = source_config.get(file_type, "")

        if not filename:
            raise ConfigurationError(
                (
                    f"File type '{file_type}' not configured for "
                    f"data source '{data_source}'"
                )
            )

        return (
            base_path / directory / "raw" / filename
            if "raw" in file_type
            else base_path / directory / "processed" / filename
        )

    def validate_config(self) -> None:
        """Validate configuration values."""
        errors: List[str] = []

        # Validate required sections
        required_sections = [
            "data_sources",
            "processing",
            "output",
            "validation",
        ]
        for section in required_sections:
            if section not in self.config:
                errors.append(
                    f"Missing required configuration section: {section}"
                )

        # Validate time range
        try:
            start_year = self.get("processing.time_range.start_year")
            end_year = self.get("processing.time_range.end_year")
            if start_year >= end_year:
                errors.append(
                    "Invalid time range: start_year must be less than end_year"
                )
        except ConfigurationError:
            errors.append("Missing time range configuration")

        # Validate data quality thresholds
        try:
            null_threshold = self.get(
                "processing.data_quality.null_threshold_percent"
            )
            if not 0 <= null_threshold <= 100:
                errors.append(
                    "Invalid null_threshold_percent: must be between 0 and 100"
                )
        except ConfigurationError:
            errors.append("Missing null threshold configuration")

        if errors:
            raise ConfigurationError(
                f"Configuration validation failed: {'; '.join(errors)}"
            )


class ConfigurationError(Exception):
    """Exception raised for configuration-related errors."""

    pass


# Global configuration instance
_config_manager = None


def get_config(
    env: str = "", config_path: Optional[Path] = None
) -> ConfigManager:
    """
    Get the global configuration manager instance.

    Args:
        env: Environment name (only used for first initialization)
        config_path: Configuration directory path
            (only used for first initialization)

    Returns:
        ConfigManager instance
    """
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager(env=env, config_path=config_path)
        _config_manager.validate_config()
    return _config_manager


def reset_config():
    """Reset the global configuration manager (mainly for testing)."""
    global _config_manager
    _config_manager = None
