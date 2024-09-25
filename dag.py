from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from etl import youtube_data_scraping, text_cleaning, sentiment_analysis

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

dag = DAG('dag', default_args=default_args, description='youtube comment analysis')

youtube_data_scraping = PythonOperator (
    task_id = 'youtube_data_scraping',
    python_callable = youtube_data_scraping,
    dag=dag
)

text_cleaning = PythonOperator (
    task_id = 'text_cleaning',
    python_callable = text_cleaning,
    dag=dag
)

sentiment_analysis = PythonOperator (
    task_id = 'sentiment_analysis',
    python_callable = sentiment_analysis,
    dag=dag
)

youtube_data_scraping >> text_cleaning >> sentiment_analysis