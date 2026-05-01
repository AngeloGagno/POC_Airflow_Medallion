from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.product.silver import silver_products
from scripts.sales.silver import silver_sales
default_args = {
    'owner': 'pato',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='silver_products',
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['duckdb', 'medallion', 'ecommerce']
) as dag:
    task_products_silver = PythonOperator(
        task_id='silver_products',
        python_callable=silver_products
    )




with DAG(
    dag_id='silver_sales',
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval='*/3 * * * *',
    catchup=False,
    tags=['duckdb', 'medallion', 'ecommerce']
) as dag:
    task_sales_silver = PythonOperator(
        task_id='silver_sales',
        python_callable=silver_sales
    )
task_products_silver
task_sales_silver