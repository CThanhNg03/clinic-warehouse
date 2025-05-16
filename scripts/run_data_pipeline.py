from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

project_root = os.getenv("PROJECT_DIR")  # /root/thanhnc
spark_job_dir = os.path.join(project_root, 'clinic-warehouse', 'data_pipeline', 'spark_job')

SPARK_MASTER = "local[*]"
SOURCE_TYPE = "local"
JAR_PATH = os.path.join(project_root, 'postgresql-42.7.4.jar')

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
            f'PYTHONPATH={project_root}/clinic-warehouse '
            f'spark-submit --master {SPARK_MASTER} '
            f'{spark_job_dir}/ingest_data.py --source {SOURCE_TYPE}'
        ),
    )

    transform_data = BashOperator(
        task_id='transform_data',
        bash_command=(
            f'PYTHONPATH={project_root}/clinic-warehouse '
            f'spark-submit --master {SPARK_MASTER} '
            f'--executor-memory 8g --driver-memory 8g '
            f'{spark_job_dir}/transform_and_write.py'
        ),
    )

    load_data = BashOperator(
        task_id='load_data',
        bash_command=(
            f'PYTHONPATH={project_root}/clinic-warehouse '
            f'spark-submit --master {SPARK_MASTER} '
            f'--executor-memory 8g --driver-memory 8g '
            f'--jars {JAR_PATH} '
            f'{spark_job_dir}/modeling.py'
        ),
    )

    extract_data >> transform_data >> load_data
