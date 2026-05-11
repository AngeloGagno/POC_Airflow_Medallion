from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.product.silver import silver_products
from scripts.sales.silver import silver_sales,consolidated_sales
from scripts.customers.silver import silver_customers
from scripts.sales.gold import gold_vendas
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
    dag_id='sales',
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
    task_consolidated_sales = PythonOperator(task_id = 'consolidated_sales', python_callable=consolidated_sales)
    task_gold_sales = PythonOperator(task_id= 'gold_sales', python_callable=gold_vendas)
    
with DAG(
    dag_id='silver_users',
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['duckdb', 'medallion', 'ecommerce']
) as dag:
    task_users_silver = PythonOperator(
        task_id='silver_customers',
        python_callable=silver_customers
    )

task_products_silver
task_sales_silver >> task_consolidated_sales >> task_gold_sales
task_users_silver
