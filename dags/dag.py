from datetime import timedelta
import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from main import twitter_elt

# configuringn airflow to run the job on a daily basis 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 1, 17),
    'email': ['twitter.email.project@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

# represents a node in the directed acyclic graph 
etl = PythonOperator(
    task_id='twitter_elt',
    python_callable=twitter_elt,
    dag=dag,
)

etl