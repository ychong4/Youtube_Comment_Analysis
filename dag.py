from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from etl import run_youtube_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,9,23),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('dag', default_args=default_args, description='youtube comment etl')

run_etl = PythonOperator (
    task_id = 'complete_youtube_etl',
    python_callable = run_youtube_etl,
    dag=dag
)
