from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import dotenv

dotenv.load_dotenv()

# Importing main functions from the scripts to enable xcom communication
import sys
sys.path.insert(0, '/opt/airflow/scripts')
from duckdb_dwh import main as DWH_CHECK_FUNC
from ingest import main as INGEST_FUNC
from export import main as EXPORT_FUNC

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ominimo_pipeline',
    default_args=default_args,
    description='Ingest, transform, and export insurance data',
    schedule=None,
    catchup=False,
) as dag:

    dwh_check = PythonOperator(
        task_id='dwh_check',
        python_callable=DWH_CHECK_FUNC,
    )

    ingest_task = PythonOperator(
        task_id='ingest_raw_data',
        python_callable=INGEST_FUNC,
    )

    dbt_build = BashOperator(
        task_id='dbt_build',
        bash_command='dbt build --select state:modified+ --state /opt/airflow/dbt_state --project-dir /opt/airflow/dbt_project/ominimo_transform --profiles-dir /opt/airflow',
        cwd='/opt/airflow/dbt_project/ominimo_transform',
    )

    export_task = PythonOperator(
        task_id='export_to_csv',
        python_callable=EXPORT_FUNC,
    )

    dwh_check >> ingest_task >> dbt_build >> export_task