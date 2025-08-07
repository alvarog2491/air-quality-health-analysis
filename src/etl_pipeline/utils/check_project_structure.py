import logging
from pathlib import Path


class CheckProjectStructure:
    """Class responsible for verifying and initializing the required
    project structure."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self) -> Path:
        """
        Verify the existence of required directories and create the output
        directory if needed.

        Returns:
            Path: Path to the main data directory.

        Raises:
            FileNotFoundError: If any required input directories are missing.
        """
        self.logger.info("Verifying project structure...")

        root_path = Path(__file__).resolve().parent.parent
        data_path = root_path / "data"

        required_dirs = [
            data_path / "air_quality_data",
            data_path / "health_data",
            data_path / "socioeconomic_data",
        ]

        # Validate required input directories
        missing_dirs = [str(p) for p in required_dirs if not p.is_dir()]
        if missing_dirs:
            raise FileNotFoundError(
                "The following required directories are missing:\n"
                + "\n".join(missing_dirs)
            )

        # Ensure the output directory exists
        output_dir = data_path / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info("Project structure successfully verified.")
        return data_path
