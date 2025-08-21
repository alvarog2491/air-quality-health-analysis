# Air Quality Health Analysis

![Tests Status](https://github.com/alvarog2491/air-quality-health-analysis/actions/workflows/tests.yml/badge.svg)

A machine learning project for analyzing the relationship between air quality and health outcomes in Spanish provinces. This project combines air quality, health, and socioeconomic datasets into a unified dataset for ML analysis.

## Project Structure

This project follows a standard ML project structure:

- **ETL Pipeline**: Complete data extraction, transformation, and loading pipeline ([ETL Documentation](src/etl_pipeline/))
- **Documentation**: Project documentation (coming soon with MkDocs)
- **Data Processing**: Air pollutants (PM2.5, PM10, NO2, SO2, O3), respiratory disease mortality, life expectancy, GDP, and population data (2000-2021)

## Installation

### Prerequisites
- Python 3.11+
- pip

### Setup
```bash
# Clone the repository
git clone <repository-url>
cd air-quality-health-analysis

# Install the project
pip install -e .

# Install with ETL dependencies
pip install -e .[etl]

# Install with development dependencies
pip install -e .[dev]
```

## Quick Start

Run the complete ETL pipeline:

```bash
# After installation, use the CLI command
air-quality-etl

# Or run manually (from project root)
cd src
python3 -m etl_pipeline.main_orchestrator
```

## Documentation

- [ETL Pipeline Documentation](src/etl_pipeline/) - Detailed ETL process documentation
- Full project documentation (MkDocs - coming soon)

## License

MIT License

## Academic Use

This project is part of a university thesis research.
