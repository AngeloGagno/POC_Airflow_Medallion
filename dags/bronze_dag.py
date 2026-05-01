from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.bronze import bronze_ecommerce
default_args = {
    'owner': 'pato',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='generate_raw',
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['duckdb', 'medallion', 'ecommerce']
) as dag:
    task_generate_raw = PythonOperator(
        task_id='generate_raw',
        python_callable=bronze_ecommerce
    )

task_generate_raw