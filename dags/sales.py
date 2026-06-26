from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.sales.bronze import bronze_sales
from scripts.sales.silver import silver_sales
default_args = {
    'owner': 'angelo',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='sales',
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval='*/3 * * * *',
    catchup=False,
    tags=['duckdb', 'medallion', 'ecommerce']
) as dag:
    task_bronze_sales = PythonOperator(
        task_id='bronze_sales',
        python_callable=bronze_sales
    )
    task_silver_sales = PythonOperator(
        task_id='silver_sales',
        python_callable=silver_sales
    )
    
task_bronze_sales >> task_silver_sales