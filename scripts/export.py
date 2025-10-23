import os
import duckdb
import logging
from dotenv import load_dotenv

load_dotenv()

log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)

_EXPORT_PATH = os.getenv('EXPORT_PATH', '/opt/airflow/data/exports/')

def _get_dwh(context) -> str:
    """Get Data warehouse location from the previous job using xcom"""

    _DWH_PATH = context['ti'].xcom_pull(task_ids="ingest_raw_data")

    return _DWH_PATH

def main(**context) -> None:
    """Export presentation layer tables to CSV for Tableau"""
    
    _DWH_PATH = _get_dwh(context)
    os.makedirs(_EXPORT_PATH, exist_ok=True)
    
    with duckdb.connect(_DWH_PATH) as conn:

        # Export tableau table
        tableau_export_file = os.path.join(_EXPORT_PATH, 'tableau_export.csv')

        conn.execute(f"""
            COPY main_presentation.kpi_business_metrics_tableau 
            TO '{tableau_export_file}' 
            (HEADER, DELIMITER ',')
        """)

        logger.info(f"Exported tableau_export to {tableau_export_file}")
    
    logger.info("Export completed successfully")


if __name__ == "__main__":
    main()