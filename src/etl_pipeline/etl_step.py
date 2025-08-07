"""
Base classes for extraction steps.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

import pandas as pd


class ETLStep(ABC):
    """Abstract base class for ETL pipeline steps."""

    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(self.__class__.__name__)
        self._recovery_enabled = True  # Enable recovery by default

    @abstractmethod
    def execute(
        self, dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]
    ) -> None:
        """Execute the ETL step."""
        pass

    def log_start(self):
        """Log step start."""
        self.logger.info(
            f"======================== Starting {self.name} "
            f"========================"
        )

    def log_success(self, message: str = ""):
        """Log step success."""
        msg = f"{self.name} completed successfully"
        if message:
            msg += f": {message}"
        self.logger.info(msg)

    @property
    def recovery_enabled(self) -> bool:
        """Check if recovery is enabled for this step."""
        return self._recovery_enabled

    @recovery_enabled.setter
    def recovery_enabled(self, value: bool):
        self._recovery_enabled = value
