# ETL Pipeline for Air Quality Health Analysis

A robust, modular ETL pipeline that combines air quality, health, and socioeconomic datasets from official Spanish sources into a unified dataset for machine learning analysis.

## Overview

This pipeline processes data on air pollutants (PM2.5, PM10, NO2, SO2, O3), respiratory disease mortality, life expectancy, GDP per capita, and population data by Spanish provinces from 2000-2021.

## Quick Start

### Prerequisites
- Python 3.11+
- Project dependencies installed

### Installation
```bash
# Install dependencies
pip install -e .

# For development with testing tools
pip install -e ".[dev]"

# For ETL with visualization dependencies  
pip install -e ".[etl]"
```

### Running the Pipeline
```bash
# Navigate to ETL pipeline directory
cd src/etl_pipeline

# Run the complete pipeline
python3 main_orchestrator.py
```

### Testing
```bash
# Run all tests
pytest tests/

# Run specific test categories
pytest tests/extract_tests/
pytest tests/transform_tests/
pytest tests/load_tests/

# Run with coverage
pytest --cov=etl_pipeline tests/
```

### Code Formatting
```bash
# Format code with Black
black .
```

## Architecture

### Pipeline Components

The pipeline follows a modular ETL architecture with these key components:

1. **ETLStep Base Class** (`etl_step.py`): Abstract base class for all pipeline steps with logging and recovery capabilities
2. **Main Orchestrator** (`main_orchestrator.py`): Coordinates the full ETL process and handles error recovery
3. **Configuration Management** (`config/`): YAML-based configuration with validation rules
4. **Logging System** (`config/logger.py`): Structured logging with file rotation

### Execution Flow

The pipeline executes steps in this order:

1. **`DataExtractionStep`** - Extract raw data from all three sources
2. **`DataTransformationStep`** - Transform individual datasets using source-specific transformers
3. **`DataMergingStep`** - Merge all datasets on Province/Year keys  
4. **`FeatureEngineeringStep`** - Create derived features (e.g., respiratory_deaths_per_100k)
5. **`DataCleaningStep`** - Remove islands, handle missing data, filter date ranges
6. **`DataValidationStep`** - Ensure data integrity and correct types
7. **`DataExportStep`** - Save final dataset to CSV
8. **`DataQualityReportStep`** - Generate comprehensive quality reports

### Data Sources

#### Air Quality Data
- **Source**: [EEA (European Environment Agency)](https://discomap.eea.europa.eu/App/AQViewer/index.html)
- **Data**: PM2.5, PM10, NO2, SO2, O3 pollutant levels across Spanish provinces
- **Classification**: [BOE quality categories](https://www.boe.es/buscar/doc.php?id=BOE-A-2020-10426) (from "buena" to "extremadamente desfavorable")
- **Extractor**: `air_quality_data_extractor.py`
- **Transformer**: `air_quality_data_transformer.py`

#### Health Data  
- **Sources**: 
  - [INE Mortality Data](https://www.ine.es/jaxiT3/Tabla.htm?t=9935&L=0): Deaths from respiratory diseases (codes 062–067)
  - [INE Life Expectancy](https://www.ine.es/jaxiT3/Tabla.htm?t=1485): Life expectancy by province and gender
- **Extractor**: `health_data_extractor.py`
- **Transformer**: `health_data_transformer.py`

#### Socioeconomic Data
- **Sources**:
  - [INE GDP Data](https://www.ine.es/dyngs/INEbase/es/operacion.htm): GDP per capita by province (2000-2022)
  - [INE Population Data](https://www.ine.es/jaxiT3/Tabla.htm?t=2852): Province population sizes
- **Extractor**: `socioeconomic_data_extractor.py`
- **Transformer**: `socioeconomic_data_transformer.py`

### Data Flow

```
Raw Data (data/{source}/raw/) 
    ↓ 
Extract Phase 
    ↓
Transform Phase (data/{source}/processed/)
    ↓
Merge & Feature Engineering
    ↓
Clean & Validate
    ↓
Final Dataset (data/output/dataset.csv)
    ↓
Quality Reports (data/output/reports/)
```

## Configuration

### Main Configuration
- **File**: `config/pipeline_config.yaml`
- **Contents**: Data sources, processing rules, validation settings, output formats

### Feature Types
- **File**: `../common/feature_types.yaml`
- **Contents**: Column type definitions and validation rules

### Province Mapping
- **File**: `utils/unified_province_name.json`
- **Contents**: Standardized province name mappings across all data sources

## Key Features

### Error Recovery System
- Built-in recovery mechanisms for validation warnings
- Configurable recovery strategies for different error types
- Graceful handling of data quality issues

### Province Name Standardization
Unified naming system handles variations like:
- `"02 Albacete"` → `"Albacete"`
- `"Alicante/Alacant"` → `"Alicante"`
- `"A Coruna"` → `"A_Coruña"`

### Data Quality Assurance
- Configurable null value thresholds
- Outlier detection and handling
- Comprehensive validation rules
- Detailed quality reporting

### Island Province Filtering
Automatically excludes island and autonomous city data:
- Santa Cruz de Tenerife, Las Palmas, Illes Balears
- Ceuta, Melilla

## Output

### Final Dataset
- **Location**: `data/output/dataset.csv`
- **Format**: Tidy data format with one row per province-year-pollutant combination
- **Columns**:
  - **Province**: Standardized province name
  - **Year**: Measurement year (2000-2021)
  - **Air_Pollutant**: Pollutant code (PM2.5, PM10, NO2, SO2, O3)
  - **Air_Pollutant_Description**: Full pollutant name
  - **Air_Pollution_Level**: Measured concentration value
  - **Unit**: Measurement unit (µg/m³)
  - **Quality**: Air quality classification
  - **Station metadata**: Type, area, coordinates, altitude
  - **Respiratory_Diseases_Total**: Total respiratory deaths
  - **Life_Expectancy**: Average life expectancy
  - **GDP_per_capita**: GDP per capita
  - **Population**: Province population
  - **respiratory_deaths_per_100k**: Calculated deaths per 100k inhabitants

### Quality Reports
- **Location**: `data/output/reports/`
- **Contents**: Data completeness, validation results, processing statistics

## Development

### Adding New ETL Steps

1. Create a new class inheriting from `ETLStep`
2. Implement the `execute(dataframes, context)` method
3. Add to the default steps list in `ETLPipeline._get_default_steps()`
4. Update configuration if needed

Example:
```python
from etl_pipeline import ETLStep

class CustomProcessingStep(ETLStep):
    def execute(self, dataframes, context):
        # Your processing logic here
        pass
```

### Adding New Data Sources

1. Create extractor in `extract/data_extractors/`
2. Create transformer in `transform/data_transformers/`
3. Follow existing patterns for province name standardization
4. Update configuration files

### Testing Strategy

- **Unit tests**: Each pipeline step has dedicated tests in `tests/*/`
- **Integration test**: End-to-end pipeline testing in `test_main_orchestrator.py`
- **Fixtures**: Shared test data in `conftest.py`
- **Markers**: Tests categorized as `unit`, `integration`, or `slow`

## Troubleshooting

### Common Issues

1. **Missing data files**: Ensure raw data files are in correct `data/{source}/raw/` directories
2. **Province name mismatches**: Check `unified_province_name.json` for proper mappings
3. **Memory issues**: Large datasets may require chunked processing
4. **Validation failures**: Review validation rules in `pipeline_config.yaml`

### Logging

Logs are written to files with rotation:
- **Location**: Timestamped log files in the pipeline directory
- **Level**: Configurable (default: INFO)
- **Format**: Structured with timestamps and component names

For more detailed debugging, set logging level to DEBUG in the configuration.