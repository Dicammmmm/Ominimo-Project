# Ominimo Data Pipeline

A reproducible ELT pipeline for insurance data using Airflow orchestration, dbt transformations, and DuckDB warehouse.

## Architecture

- **Orchestration**: Apache Airflow 3.1.0
- **Transformation**: dbt Core 1.10.13
- **Data Warehouse**: DuckDB 1.4.1
- **Containerization**: Docker & Docker Compose

## Pipeline Flow

1. **DWH Check**: Verifies or creates DuckDB warehouse with schemas (raw, staging, mart, presentation)
2. **Ingestion**: Loads policies and quotes parquet files into raw schema
3. **dbt Transform**: Executes staging, mart, and presentation models
4. **dbt Test**: Runs data quality tests on transformed data
5. **Export**: Generates CSV file for Tableau Public visualization

## Project Structure

```
.
├── dags/                    # Airflow DAG definitions
├── scripts/                 # Python ingestion/export scripts
├── dbt_project/            # dbt transformation logic
│   └── ominimo_transform/
│       ├── models/
│       │   ├── staging/    # Type casting, basic cleaning
│       │   ├── mart/       # Business logic layer
│       │   └── presentation/ # Aggregations and KPIs
│       └── dbt_project.yml
├── data/                   # DuckDB warehouse and raw files
├── config/                 # Airflow configuration
├── profiles.yml            # dbt profile configuration
├── docker-compose.yaml
├── dockerfile
└── requirements.txt
```

## Data Models

### Staging Layer
- `stg_policies`: Type-cast policy records with metadata
- `stg_quotes`: Type-cast quote records with metadata

### Mart Layer
- `fct_policy_quotes`: Joined fact table (policies LEFT JOIN quotes)

### Presentation Layer
- `kpi_business_metrics`: Aggregated KPIs by product, status, and vehicle attributes
- `kpi_business_metrics_tableau`: Row-level data with conversion flags for visualization

## Prerequisites

- Docker Desktop
- Docker Compose
- 4GB RAM minimum
- 10GB disk space

## Setup Instructions

### 1. Clone Repository

```bash
git clone <repository-url>
cd ominimo-pipeline
```

### 2. Prepare Environment

Create `.env` file:

```bash
DWH_NAME=ominimo_dwh.duckdb
DWH_FOLDER_PATH=/opt/airflow/data/
RAW_FILES_PATH=/opt/airflow/data/
EXPORT_PATH=/opt/airflow/data/exports/
```

### 3. Add Data Files

Place `policies.parquet` and `quotes.parquet` in `./data/` directory.

### 4. Build and Start Services

```bash
docker-compose up --build -d
```

This initializes:
- PostgreSQL metadata database
- Airflow API server (port 8080)
- Airflow scheduler
- Airflow DAG processor
- Airflow triggerer

### 5. Access Airflow UI

Navigate to `http://localhost:8080`

Credentials:
- Username: `airflow`
- Password: `airflow`

### 6. Run Pipeline

1. In Airflow UI, locate `ominimo_pipeline` DAG
2. Unpause the DAG (toggle switch)
3. Trigger manually (play button)

Pipeline tasks execute in sequence:
```
dwh_check → ingest_raw_data → dbt_transform → dbt_test → export_to_csv
```

### 7. Access Output

CSV export available at: `./data/exports/tableau_export.csv`

## Data Quality Tests

dbt tests validate:
- Unique constraints (policy_id, quote_id)
- Not null constraints
- Accepted values (status, product)
- Referential integrity (policy-quote relationships)

Run tests separately:
```bash
docker exec -it <scheduler-container> bash
dbt test --project-dir /opt/airflow/dbt_project/ominimo_transform --profiles-dir /opt/airflow
```

## Key Features

### Ingestion Metadata
All staging tables include:
- `source_file`: Origin file name
- `loaded_at`: Ingestion timestamp

### Business Metrics
`kpi_business_metrics` provides:
- `total_quotes`: Unique quote count
- `total_policies`: Unique policy count
- `avg_premium`: Average quoted price
- `avg_technical_price`: Average technical price
- `conversion_ratio`: Policies divided by quotes
- `min_premium`, `max_premium`: Price boundaries
- `total_premium_value`: Sum of all premiums

### Tableau Export
`tableau_export.csv` contains row-level data with:
- `price_markup`: Difference between price and technical_price
- `converted_flag`: Binary indicator (1 = quote converted to policy)

## Troubleshooting

### Container Issues
```bash
# View logs
docker-compose logs -f

# Restart services
docker-compose restart

# Full reset
docker-compose down -v
docker-compose up --build -d
```

### Permission Errors
Ensure proper ownership of mounted volumes:
```bash
sudo chown -R 50000:0 ./logs ./data ./dags ./plugins
```

### DAG Not Appearing
- Verify DAG file syntax
- Check scheduler logs for parsing errors
- Ensure `dags/` directory is mounted correctly

## Stopping Services

```bash
docker-compose down
```

Preserve data:
```bash
docker-compose down
```

Remove all data:
```bash
docker-compose down -v
```

## Technical Decisions

### Why DuckDB?
- Embedded database requires no separate server
- Native parquet support for efficient ingestion
- Sufficient for demonstration-scale workloads
- Easy local reproducibility

### Why CSV Export?
- Tableau Public lacks connector support for DuckDB
- CSV provides universal compatibility
- Export task runs post-transformation to ensure data quality

### Why LocalExecutor?
- Single-machine deployment simplifies setup
- No Redis/Celery dependencies
- Adequate for bounded pipeline workload

### Schema Strategy
dbt uses separate schemas (staging, mart, presentation) to maintain clear separation of concerns and enable selective refreshes in production scenarios.
