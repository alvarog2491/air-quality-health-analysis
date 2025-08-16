# ETL Pipeline for Air Quality Health Analysis

A modular ETL pipeline that combines air quality, health, and socioeconomic datasets from official Spanish sources into a unified dataset for machine learning analysis.

## Quick Start

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
```

## Data Sources

### Air Quality Data
- **Source**: [EEA (European Environment Agency)](https://discomap.eea.europa.eu/App/AQViewer/index.html)
- **Data**: PM2.5, PM10, NO2, SO2, O3 pollutant levels across Spanish provinces
- **Classification**: [BOE quality categories](https://www.boe.es/buscar/doc.php?id=BOE-A-2020-10426) (from "buena" to "extremadamente desfavorable")

### Health Data  
- **Sources**: 
  - [INE Mortality Data](https://www.ine.es/jaxiT3/Tabla.htm?t=9935&L=0): Deaths from respiratory diseases (codes 062–067)
  - [INE Life Expectancy](https://www.ine.es/jaxiT3/Tabla.htm?t=1485): Life expectancy by province and gender

### Socioeconomic Data
- **Sources**:
  - [INE GDP Data](https://www.ine.es/dyngs/INEbase/es/operacion.htm): GDP per capita by province (2000-2022)
  - [INE Population Data](https://www.ine.es/jaxiT3/Tabla.htm?t=2852): Province population sizes

## Pipeline Execution

The pipeline executes the following steps in order:

1. **`DataExtractionStep`** - Extract raw data from all three sources
2. **`DataTransformationStep`** - Transform individual datasets using source-specific transformers
3. **`DataMergingStep`** - Merge all datasets on Province/Year keys  
4. **`FeatureEngineeringStep`** - Create derived features (e.g., respiratory_deaths_per_100k)
5. **`DataCleaningStep`** - Remove islands, handle missing data, filter date ranges
6. **`DataValidationStep`** - Ensure data integrity and correct types
7. **`DataExportStep`** - Save final dataset to CSV
8. **`DataQualityReportStep`** - Generate quality reports

## Data Flow

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

### Main Configuration Files
- **`config/pipeline_config.yaml`**: Data sources, processing rules, validation settings
- **`../common/feature_types.yaml`**: Column type definitions and validation rules
- **`utils/unified_province_name.json`**: Standardized province name mappings

## Key Features

### Province Name Standardization
Unified naming system handles variations across data sources:
- `"02 Albacete"` → `"Albacete"`
- `"Alicante/Alacant"` → `"Alicante"`
- `"A Coruna"` → `"A_Coruña"`

### Island Province Filtering
Automatically excludes island and autonomous city data:
- Santa Cruz de Tenerife, Las Palmas, Illes Balears
- Ceuta, Melilla

### Error Recovery System
- Built-in recovery mechanisms for validation warnings
- Configurable recovery strategies for different error types
- Graceful handling of data quality issues

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

## Architecture

### Key Components
- **ETLStep Base Class** (`etl_step.py`): Abstract base class for all pipeline steps with logging and recovery capabilities
- **Main Orchestrator** (`main_orchestrator.py`): Coordinates the full ETL process and handles error recovery
- **Source-specific extractors**: `extract/data_extractors/`
- **Source-specific transformers**: `transform/data_transformers/`

### Adding New ETL Steps
1. Create a new class inheriting from `ETLStep`
2. Implement the `execute(dataframes, context)` method
3. Add to the default steps list in `ETLPipeline._get_default_steps()`
4. Update configuration if needed

### Adding New Data Sources
1. Create extractor in `extract/data_extractors/`
2. Create transformer in `transform/data_transformers/`
3. Follow existing patterns for province name standardization
4. Update configuration files

## Troubleshooting

### Common Issues
1. **Missing data files**: Ensure raw data files are in correct `data/{source}/raw/` directories
2. **Province name mismatches**: Check `unified_province_name.json` for proper mappings
3. **Validation failures**: Review validation rules in `pipeline_config.yaml`

### Logging
- **Location**: Timestamped log files in the pipeline directory
- **Level**: Configurable (default: INFO)
- **Format**: Structured with timestamps and component names