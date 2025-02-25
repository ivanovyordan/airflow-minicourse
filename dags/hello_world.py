# dags/hello_world.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("Hello, World!")

with DAG(
    dag_id='hello_world',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description='A simple Hello World DAG'
) as dag:
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=say_hello
    )
