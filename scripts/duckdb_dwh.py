import os
import dotenv
import duckdb
import logging

dotenv.load_dotenv()


log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)

_DWH_NAME = os.getenv('DWH_NAME')
_DWH_FOLDER_PATH = os.getenv('DWH_FOLDER_PATH', "/opt/airflow/data/")


def main() -> str:
    db_files = [f for f in os.listdir(_DWH_FOLDER_PATH) if f.endswith(('.duckdb', '.db'))]

    if db_files:
        _DWH_PATH = os.path.join(_DWH_FOLDER_PATH, db_files[0])
        logger.info(f"Data warehouse already exists: {_DWH_PATH}")

    else:
        logger.info("Data warehouse not found, creating a new one.")
        _DWH_PATH = os.path.join(_DWH_FOLDER_PATH, _DWH_NAME)
        
        with duckdb.connect(_DWH_PATH) as conn:
            conn.sql("CREATE SCHEMA IF NOT EXISTS raw")
            conn.sql("CREATE SCHEMA IF NOT EXISTS main_staging")
            conn.sql("CREATE SCHEMA IF NOT EXISTS main_mart")
            conn.sql("CREATE SCHEMA IF NOT EXISTS main_presentation")
    
    logger.info(f"Warehouse path: {_DWH_PATH}")
    return _DWH_PATH

if __name__ == "__main__":
    main()