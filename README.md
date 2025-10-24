# Ominimo Data Pipeline

A reproducible ELT pipeline for insurance data processing with Airflow orchestration, dbt transformations, and DuckDB warehouse.

## Architecture

- **Orchestration**: Apache Airflow 3.1.0
- **Transformation**: dbt Core 1.10.13
- **Warehouse**: DuckDB 1.4.1
- **Container**: Docker & Docker Compose

## Pipeline Flow
```
dwh_check → ingest_raw_data → dbt_transform → dbt_test → export_to_csv
```

1. **DWH Check**: Create/verify DuckDB warehouse with schemas
2. **Ingestion**: Load parquet files into raw schema
3. **dbt Transform**: Execute staging → mart → presentation models
4. **dbt Test**: Run data quality validations
5. **Export**: Generate CSV for Tableau Public

## Project Structure
```
.
├── dags/                    # Airflow DAG definitions
├── scripts/                 # Python ingestion/export scripts
├── dbt_project/
│   └── ominimo_transform/
│       └── models/
│           ├── staging/     # Type casting, cleaning
│           ├── mart/        # Business logic (fact table)
│           └── presentation/ # KPIs and aggregations
├── data/                    # DuckDB warehouse & raw files
├── config/                  # Airflow configuration
└── profiles.yml             # dbt profile
```

## Data Models

**Staging**: `stg_policies`, `stg_quotes` (typed with ingestion metadata)  
**Mart**: `fct_policy_quotes` (policies LEFT JOIN quotes)  
**Presentation**: 
- `kpi_business_metrics` (aggregated KPIs)
- `kpi_business_metrics_tableau` (row-level with conversion flags)

## Prerequisites

- Docker Desktop
- Docker Compose
- 4GB RAM minimum

## Setup & Execution

1. **Clone repository**
```bash
git clone <repository-url>
cd ominimo-pipeline
```

2. **Create `.env` file**
```bash
DWH_NAME=ominimo_dwh.duckdb
DWH_FOLDER_PATH=/opt/airflow/data/
RAW_FILES_PATH=/opt/airflow/data/raw/
EXPORT_PATH=/opt/airflow/data/exports/
AIRFLOW_UID=50000
```

3. **Add data files**  
Place `policies.parquet` and `quotes.parquet` in `./data/`

4. **Start services**
```bash
docker-compose up --build -d
```

5. **Access Airflow UI**  
Navigate to `http://localhost:8080`  
Credentials: `airflow` / `airflow`

6. **Run pipeline**  
Unpause and trigger `ominimo_pipeline` DAG

7. **Access output**  
CSV available at `./data/exports/tableau_export.csv`

## Key Features

- **Ingestion metadata**: `source_file`, `loaded_at` on all staging tables
- **Business metrics**: conversion_ratio, avg_premium, total_premium_value
- **Profit calculation**: `price_markup` (price - technical_price)
- **Data quality tests**: uniqueness, nullability, accepted values
- **Reproducibility**: Fully containerized with mounted volumes

## Stopping Services
```bash
docker-compose down      # Preserve data
docker-compose down -v   # Remove all data
```
