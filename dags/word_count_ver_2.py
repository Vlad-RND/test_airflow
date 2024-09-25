import statistics

from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor


operations = {
    'min': min,
    'max': max,
    'sum': sum,
    'average': statistics.mean
}

def process_raw_fn():
    with open('/opt/airflow/dags/data/raw_data.txt', 'r') as inp:
        with open('/opt/airflow/dags/data/processed_data.txt', 'w') as out:
            while True:
                line = inp.readline()
                if not line:
                    break
                result_list = []
                for number in line.split():
                    try:
                        result_list.append(float(number))
                    except ValueError:
                        continue
            operation = Variable.get("operation", default_var=None)
            if not result_list:
                result = "Нет чисел для выполнения операции."
            else:
                try:
                    result = operations[operation](result_list)
                except KeyError:
                    result = f"Операция '{operation}' не поддерживается."
            out.write(str(result))
    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='task_new',
    catchup=False,
    start_date=datetime(2024, 9, 19),
    schedule=None,
    tags=['stream_3'],
    default_args=default_args,
) as dag_1:
    sensor_task = FileSensor(task_id='wait_for_file', filepath='/opt/airflow/dags/data/raw_data.txt', poke_interval=10)
    process_raw_task = PythonOperator(task_id='process_raw_task', python_callable=process_raw_fn, outlets=[Dataset('/opt/airflow/dags/data/processed_data.txt')])
    move_to_trash_task = BashOperator(task_id='move_to_trash', bash_command='mv --backup=t /opt/airflow/dags/data/raw_data.txt /opt/airflow/dags/data/trash/',)
    start_new_task = TriggerDagRunOperator(task_id='start_new', trigger_dag_id='task')
    log_info_task = BashOperator(task_id='log_info_task', bash_command='echo {{ ti.xcom_pull(task_ids="process_raw_task") }}')

    sensor_task >> process_raw_task >> [move_to_trash_task, start_new_task, log_info_task]
