from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_hello_world',
    default_args=default_args,
    description='No',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10
)

def foo(name):
    print(f'Hello {name}!')
    return 1

task_1 = PythonOperator(
    task_id='hello_world', python_callable=foo, op_kwargs={'name': 'Vlad'}, dag=dag
)