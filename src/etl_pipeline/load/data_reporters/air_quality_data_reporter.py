import logging
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


class AirQualityDataReporter:
    """Generates and saves air quality data reports."""

    def __init__(self, output_dir: str = "reports"):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.output_dir = output_dir

    def report_air_pollutant_level(
        self, df: pd.DataFrame, filename: str = "air_pollution_level.png"
    ):
        plt.figure(figsize=(15, 5))
        sns.lineplot(data=df, x="Year", y="Air Pollution Level")
        plt.title("Air Pollution Level")
        plt.tight_layout()
        self._save_plot(filename)

    def _save_plot(self, filename: str):
        import os

        os.makedirs(self.output_dir, exist_ok=True)
        path = os.path.join(self.output_dir, filename)
        plt.savefig(path)
        plt.close()
        self.logger.info(f"Plot saved to {path}")
