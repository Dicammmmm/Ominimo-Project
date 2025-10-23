import os
import dotenv
import duckdb
import logging

dotenv.load_dotenv()

log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)

_RAW_PATH = os.getenv("RAW_FILES_PATH", "/opt/airflow/data/")

_POLICIES_FILE = os.path.join(_RAW_PATH, "policies.parquet")
_QUOTES_FILES = os.path.join(_RAW_PATH, "quotes.parquet")


def _get_dwh(context) -> str:
    """Get Data warehouse location from the previous job using xcom"""

    _DWH_PATH = context['ti'].xcom_pull(task_ids="dwh_check")

    return _DWH_PATH

def main(**context) -> None:
    _DWH_PATH = _get_dwh(context)

    logger.info(f"Using data warehouse: {_DWH_PATH}")

    with duckdb.connect(_DWH_PATH) as conn:
        conn.sql(
            f"""
                CREATE OR REPLACE TABLE raw.policies_raw AS
                SELECT * FROM read_parquet('{_POLICIES_FILE}')
            """
        )
        logger.info(f"Created table raw.policies_raw from {_POLICIES_FILE}")

        conn.sql(
            f"""
                CREATE OR REPLACE TABLE raw.quotes_raw AS
                SELECT * FROM read_parquet('{_QUOTES_FILES}')
            """
        )
        logger.info(f"Created table raw.quotes_raw from {_QUOTES_FILES}")

    logger.info("Data loaded successfully into the warehouse.")

    return _DWH_PATH

if __name__ == "__main__":
    main()