from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
spark_job_dir = os.path.abspath(os.path.join(current_dir, '..', 'data_pipeline/spark_job'))

SPARK_MASTER = "local[*]" 
SOURCE_TYPE = "local"  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ETL_Pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    extract_data = BashOperator(
        task_id='extract_data',
        bash_command=(
            f'spark-submit --master {SPARK_MASTER} '
            f'{spark_job_dir}/ingest_data.py --source {SOURCE_TYPE}'
        ),
    )

    transform_data = BashOperator(
        task_id='transform_data',
        bash_command=(
            f'spark-submit --master {SPARK_MASTER} '
            f'{spark_job_dir}/transform_and_write.py'
        ),
    )

    load_data = BashOperator(
        task_id='load_data',
        bash_command=(
            f'spark-submit --master {SPARK_MASTER} '
            f'{spark_job_dir}/modeling.py'
        ),
    )

    extract_data >> transform_data >> load_data
