import numpy as np
from typing import Dict, List, Any

quality_thresholds: Dict[str, list[Any]] = {
    "so2": [0, 100, 200, 350, 500, 750, np.inf],
    "pm2.5": [0, 10, 20, 25, 50, 75, np.inf],
    "pm10": [0, 20, 40, 50, 100, 150, np.inf],
    "o3": [0, 50, 100, 130, 240, 380, np.inf],
    "no2": [0, 40, 90, 120, 230, 340, np.inf],
}

quality_labels: List[str] = [
    "BUENA",
    "RAZONABLEMENTE BUENA",
    "REGULAR",
    "DESFAVORABLE",
    "MUY DESFAVORABLE",
    "EXTREMADAMENTE DESFAVORABLE",
]
