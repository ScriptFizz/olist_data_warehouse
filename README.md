# Brazilian Ecommerce Data Warehouse & Analytics Project

End-to-end data warehouse and analytics project built on the Olist Brazilian Ecommerce dataset,  
showcasing data ingestion, validation, transformation, modeling, and analytical insights  
using modern data engineering and analytics tools.

## Project Overview

This project implements a complete data pipeline and analytics workflow using the public Olist  
Brazilian Ecommerce dataset.  
The goal is to simulate a real-world data warehouse environment and demonstrate best practices 
analytics modeling and exploratory data analysis.

The project covers: 
 - Data ingestion and validation 
 - Data transformation and warehouse modeling 
 - Analytics and BI layer creation 
 - Exploratory and business-oriented data analysis

## Architecture and Data Flow

The project follows a layered data architecture: 

1. **Raw Layer**
  - Ingest CSV datasets from Kaggle and external exchange rate data 
  - Schema validation using Pandera 

2. **Processed Layer** 
  - Cleaned and standardized datasets 
  - Data quality checks enforced via Pandera schema 
  - Stored locally and prepared for warehouse loading 

3. **Data Warehouse (BigQuery)** 
  - **Core Layer**: Fact and dimension tables 
  - **Rollup Layer**: Additive aggregate facts 
  - **KPI/Semantic Layer**: Non-additive metrics built on top of rollup
  - **BI Layer**: Aggregated KPI views for reporting and dashboards 

4. **Analytics & Visualization** 
  - SQL-based analytics in BigQuery 
  - Jupyter notebooks for exploratory analysis and visualization 

## Tech Stack 

**Data Engineering** 
 - Python (pandas)
 - Pandera (data validation)
 - BigQuery (data warehouse)
 - SQL (analytics and BI modeling)
 - Typer (CLI interface)
 - Poetry (dependency management) 

**Analytics & Visualization** 
 - Jupyter Notebook
 - seaborn
 - matplotlib


**Tooling & Quality** 
 - MKDocs (project documentation) 
 - Ruff, MyPy (linting and type checking) 
 - pre-commit
 - Logging and configuration management 
 
## Repository Structure 

```text
data/          # Raw and processed datasets
src/           # ETL, validation, and CLI logic
sql/           # Core, analytics, and BI SQL models
notebooks/     # Exploratory and analytical notebooks
DA_report      # PDF of Business Insights report on Looker Studio
docs/          # Project documentation (MkDocs)
logs/          # Application logs
tests/         # Test scaffolding
```

## ETL Pipeline and CLI usage 

The ETL pipeline is fully subscriptbale via a command-line interface built with Typer. 

Typical workflow:
1. Extract raw datasets and external exchange rates
2. Validate schemas using Pandera 
3. Transform data into processed datasets 
4. Load data into BigQuery datasets 

Example commands: 

```bash
python -m src.cli.extract_csv_cli 
python -m src.cli.extract_rates_cli 
python -m src.cli.transform_cli 
python -m src.cli.load_bigquery_cli
python -m src.cli.load_layers_cli
``` 

## Data Availability 

Raw and processed data files are intentionally excluded from version control. 

The project is fully reproducible: 
 - Raw data can be downloaded from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) 
 - Processed data is generated via the ETL pipeline 
 - Warehouse tables are built programmatically in BigQuery 
 
See `docs/engineering/etl` for details on the data ingestion process.

## Data Modeling

The BigQuery warehouse follows a layered dimensional modeling approach, progressing
from source-aligned ingestion to additive aggregates and semantic KPIs optimized for BI
and analytics use cases.

- **Raw Layer**
  - Standardized ingestion tables with consistent data types and keys
  - Preserves source-level grain and business meaning
  - Serves as the foundation for dimensional modeling

- **Core Layer**
  - **Atomic fact tables** at the lowest business grain  
    (e.g. orders, order_items, payments, reviews)
  - **Conformed dimensions**  
    (customers, sellers, products, dates, geography)
  - Designed for reuse across downstream rollups and KPIs

- **Rollup Layer (Additive Aggregate Facts)**
  - Aggregate fact tables containing **additive-only measures** (`sum`, `count`)
  - Optimized for performance and reuse in KPI calculations
  - Includes:
    - Customer order rollups
    - Seller performance rollups
    - Product sales rollups
    - Geographic demand and supply rollups

- **KPI / Semantic Layer**
  - Derived tables/views built on top of additive rollups
  - Contains **non-additive metrics**, ratios, scores, and segmentation logic
  - Includes:
    - Customer RFM and behavioral KPIs
    - Customer status and segmentation models
    - Seller and product performance KPIs
    - Daily business KPIs
  - Designed as a stable semantic contract for BI and analytics consumers

- **BI / Presentation Layer**
  - Final presentation-ready views for dashboards and reporting
  - Minimal business logic; focuses on usability and consistency 

## Analysis & Key Insights 

Exploratory and business-oriented analyses are conducted in Jupyter notebook 
using SQL queries against the BigQuery warehouse and Python visualizations. 

Key insights include: 

 - Demand seasonality shows stronger response to promotion rather than holidays 
 - Delivery delays have a systematic negative impact on customer review scores 
 - Revenue is concentrated in a small number of personal and household care categories 

See: `notebooks/Brazilian_Ecommerce_Analysis.ipynb` 

A summary of the analysis with key business insights is conducted on a Looker Studio 
report, whose static PDF copy is available.  

See: `DA_report/Olist_Marketplace_Report.pdf`

## Documentation 

Technical documentation is available via MKDocs and covers: 
 - ETL pipeline design 
 - CLI usage 
 - Metrics definition 

See the `docs/` directory or build the documentation locally using MKDocs. 

## Future improvements 

 - Introduce orchestration (e.g. Airflow or Prefect) 
 - link to dashboarding layer (Looker Studio) 
 - Implement incremental loading strategies 
