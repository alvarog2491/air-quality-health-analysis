# ETL Pipeline Documentation

The ETL (Extract, Transform, Load) pipeline processes raw data from multiple sources into a unified dataset suitable for machine learning analysis.

## Overview

The pipeline processes data from three main sources:

- **Air Quality Data**: Pollutant measurements with station metadata
- **Health Data**: Respiratory disease mortality and life expectancy statistics
- **Socioeconomic Data**: GDP (PIB) and population data

All data covers Spanish provinces from 2000-2021 and is transformed into a single, clean dataset.

## Pipeline Architecture & Data Flow

The pipeline follows a modular design with 8 sequential steps, each inheriting from the `ETLStep` base class:

<div align="center">

```mermaid
flowchart TD
    subgraph "Input Data"
        A1[Air Quality CSVs<br/>Station measurements]
        A2[Health CSVs<br/>Province mortality/life expectancy] 
        A3[Socioeconomic CSVs<br/>PIB/Population by province]
    end
    
    subgraph "ETL Processing"
        B1[<b>DataExtractionStep</b><br/>Extract & Load Raw DataFrames]
        B2[<b>DataTransformationStep</b><br/>Transform & Standardize]
        B3[<b>DataMergingStep</b><br/>Merge on Province+Year]
        B4[<b>FeatureEngineeringStep</b><br/>Create calculated features]
        B5[<b>DataCleaningStep</b><br/>Clean & filter data]
        B6[<b>DataValidationStep</b><br/>Ensure data quality]
        B7[<b>DataExportStep</b><br/>Save final dataset]
        B8[<b>DataQualityReportStep</b><br/>Generate reports]
    end
    
    subgraph "Output"
        C1[Unified Dataset<br/>Province-Year-Pollutant records]
        C2[Quality Reports<br/>HTML/JSON]
        C3[Metadata & Dictionary<br/>CSV/JSON]
    end
    
    A1 --> B1
    A2 --> B1
    A3 --> B1
    B1 --> B2
    B2 --> B3
    B3 --> B4
    B4 --> B5
    B5 --> B6
    B6 --> B7
    B7 --> B8
    B7 --> C1
    B7 --> C3
    B8 --> C2
```

</div>

## ETL Steps in Detail

### 1. DataExtractionStep
**Purpose**: Read raw CSV files from different data sources

<div align="center">

```mermaid
graph LR
    A[data/air_quality/raw/] --> D[Air Quality DataFrames]
    B[data/health/raw/] --> E[Health DataFrames]
    C[data/socioeconomic/raw/] --> F[Socioeconomic DataFrames]
    
    style D fill:#e1f5fe
    style E fill:#f3e5f5
    style F fill:#e8f5e8
```

</div>

- Loads CSV files from `data/{source}/raw/` directories
- Creates separate DataFrames for each data source
- Performs initial data type detection and basic validation

### 2. DataTransformationStep
**Purpose**: Transform and standardize data from each source

<div align="center">

```mermaid
graph TD
    A[Air Quality DataFrames] --> B[Air Quality Transformer]
    C[Health DataFrames] --> D[Health Transformer]
    E[Socioeconomic DataFrames] --> F[Socioeconomic Transformer]
    
    B --> G[Standardized Air Quality Data]
    D --> H[Standardized Health Data]
    F --> I[Standardized Socioeconomic Data]
    
    style G fill:#e1f5fe
    style H fill:#f3e5f5
    style I fill:#e8f5e8
```

</div>

**Key transformations:**

- **Air Quality**: 
    - Clean invalid Province values
    - Classify air quality levels using pollutant thresholds

- **Health**: 
    - Rename columns (Provincias→Province, Periodo→Year, Total→target metrics)
    - Remove commas/dots from respiratory disease totals
    - Convert to numeric types

- **Socioeconomic**: 
    - Transform GDP from wide to long format
    - Rename columns (value→pib, Provincia→Province, anio→Year)
    - Remove dots from population numbers
    - Convert date formats

**Common transformations applied to all sources:**

- Province name standardization using unified mapping system
- Date/year column standardization to consistent format

!!! info "Province Name Standardization"
    Each data source uses different province naming conventions, but Province+Year serves as the primary key for merging. All sources must share identical province names using a unified mapping system.

### 3. DataMergingStep
**Purpose**: Combine all data sources into a single DataFrame

<div align="center">

```mermaid
graph TD
    A[Air Quality Data] --> D{Merge on<br/>Province + Year}
    B[Health Data] --> D
    C[Socioeconomic Data] --> D
    D --> E[Unified DataFrame]
```

</div>

**Merge process:**

- Uses Province and Year as composite key
- Maintains air quality station metadata
- Preserves pollutant-specific measurements
- Results in multiple rows per province-year (one per pollutant type)

### 4. FeatureEngineeringStep
**Purpose**: Create new calculated columns

<div align="center">

```mermaid
graph TD
    A[Merged Data] --> B[Calculate respiratory_deaths_per_100k]
    B --> C[Enhanced Dataset]
```

</div>

**Generated feature:**
- `respiratory_deaths_per_100k`: Calculates respiratory deaths per 100,000 population from respiratory disease totals and population data

### 5. DataCleaningStep
**Purpose**: Clean and filter the dataset

<div align="center">

```mermaid
graph TD
    A[Enhanced Dataset] --> B[Remove Metadata Columns]
    B --> C[Remove Island Observations]
    C --> D[Filter Timeframe]
    D --> E[Convert Categories to Lowercase]
    E --> F[Handle Null Values]
    F --> G[Remove Duplicates]
    G --> H[Convert Data Types]
    H --> I[Standardize Column Names]
    I --> J[Clean Dataset]
```

</div>

**Cleaning operations:**

- Remove metadata columns (e.g., "Total Nacional", "Comunidades y Ciudades Autónomas")
- Remove island observations (excluded regions from configuration)
- Filter timeframe to configured date range
- Convert categorical columns to lowercase (except Province)
- Handle null values (<5% removed, >5% kept for imputation)
- Remove duplicate rows
- Convert columns to appropriate data types from feature_types.yaml
- Standardize column names to lowercase with underscores

### 6. DataValidationStep
**Purpose**: Ensure data quality and integrity

<div align="center">

```mermaid
graph TD
    A[Clean Dataset] --> B[Validate Not Empty]
    B --> C[Validate Nulls]
    C --> D[Validate Data Types]
    D --> E[Validate Duplicates]
    E --> F[Validate Required Columns]
    F --> G[Validate Business Rules]
    G --> H[Detect Statistical Anomalies]
    H --> I{Validation Results}
    I -->|Pass| J[Validated Dataset]
    I -->|Warnings| K[Log Issues & Continue]
    I -->|Errors| L[Pipeline Failure]
```

</div>

**Validation checks:**

- **Not Empty**: Ensure DataFrame has data and rows
- **Nulls**: Check null percentage against configured thresholds
- **Data Types**: Verify columns match feature_types.yaml schema
- **Duplicates**: Detect duplicate rows (configurable allowance)
- **Required Columns**: Ensure all required columns are present
- **Business Rules**: Validate year ranges and positive pollution levels
- **Statistical Anomalies**: Detect outliers using IQR method (>10% triggers warning)

### 7. DataExportStep
**Purpose**: Save the final dataset to storage

<div align="center">

```mermaid
graph TD
    A[Validated Dataset] --> B[Export to CSV]
    A --> C[Generate Metadata]
    A --> D[Create Data Dictionary]
    B --> E[data/output/dataset.csv]
    C --> F[data/output/metadata.json]
    D --> G[data/output/data_dictionary.csv]
```

</div>

**Export outputs:**

- Main dataset: `data/output/dataset.csv`
- Metadata file with processing statistics
- Data dictionary with column descriptions
- Processing logs and timestamps

### 8. DataQualityReportStep
**Purpose**: Generate quality reports

<div align="center">

```mermaid
graph TD
    A[Final Dataset] --> B[Statistical Summary]
    A --> C[Missing Data Report]
    A --> D[Distribution Analysis]
    A --> E[Geographic Analysis]
    B --> F[Quality Report HTML]
    C --> F
    D --> F
    E --> F
    F --> G[data/output/reports/]
```

</div>

**Report contents:**

- Descriptive statistics for all variables
- Missing data patterns and percentages
- Pollutant distribution analysis
- Geographic coverage and station distribution
- Data quality scores and recommendations

## Configuration

The pipeline uses YAML configuration files:

- `config/pipeline_config.yaml`: Main pipeline settings
- `utils/unified_province_name.json`: Province name mappings
- `../common/feature_types.yaml`: Feature type definitions

## Error Handling & Recovery

The pipeline includes built-in error recovery mechanisms:

- **Validation warnings**: Continue processing with logged warnings
- **Missing data**: Apply configured imputation strategies  
- **Processing errors**: Retry with alternative approaches
- **Critical failures**: Stop pipeline with detailed error reporting

## Testing

Test suite that covers all pipeline components:

```bash
# Run all ETL tests
pytest tests/

# Run specific test categories
pytest tests/extract_tests/
pytest tests/transform_tests/
pytest tests/load_tests/
```

## Output Dataset Schema

The final dataset contains the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `air_pollutant` | string | Pollutant type (no2, pm25, pm10, so2, o3) |
| `air_pollutant_description` | string | Full pollutant description |
| `data_aggregation_process` | string | How data was aggregated (e.g., "annual mean / 1 calendar year") |
| `year` | date | Measurement year (YYYY-01-01 format) |
| `air_pollution_level` | float | Pollutant concentration value |
| `unit_of_air_pollution_level` | string | Measurement unit (typically μg/m³) |
| `air_quality_station_type` | string | Station type (traffic, background, industrial) |
| `air_quality_station_area` | string | Area type (urban, suburban, rural) |
| `longitude` | float | Station longitude coordinate |
| `latitude` | float | Station latitude coordinate |
| `altitude` | float | Station altitude (meters) |
| `province` | string | Standardized Spanish province name |
| `quality` | string | Data quality classification (buena, etc.) |
| `respiratory_diseases_total` | integer | Total respiratory disease deaths |
| `life_expectancy_total` | float | Life expectancy at birth |
| `pib` | float | PIB (GDP) in billions of euros |
| `population` | integer | Province population |
| `respiratory_deaths_per_100k` | float | Respiratory deaths per 100,000 population |
