import statistics

from datetime import datetime, timedelta
import json

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
    distinct_words = set()

    with open('/opt/airflow/dags/data/raw_data.txt', 'r') as inp:
        with open('/opt/airflow/dags/data/processed_data.txt', 'w') as out:
            while True:
                line = inp.readline()
                if not line:
                    break
                result = {}
                for word in line.split():
                    result[word] = result.get(word, 0) + 1
                distinct_words = distinct_words.union(set(result.keys()))
                out.write(json.dumps(result) + '\n')
    return len(distinct_words)

## Word count version
# def combine_lines():
#     with open('/opt/airflow/dags/data/processed_data.txt', 'r') as inp:
#         with open('/opt/airflow/dags/data/word_counts.txt', 'w') as out:
#             result = {}
#             while True:
#                 line = inp.readline()
#                 if not line:
#                     break
#                 row_data = json.loads(line)
#                 for word, cnt in row_data.items():
#                     result[word] = result.get(word, 0) + cnt
#             out.write('\n'.join(f'{key}={value}' for key, value in result.items()))

# Numbers count version
def combine_lines():
    with open('/opt/airflow/dags/data/processed_data.txt', 'r') as inp:
        with open('/opt/airflow/dags/data/word_counts.txt', 'w') as out:
            result_list = []
            while True:
                line = inp.readline()
                if not line:
                    break
                row_data = json.loads(line)
                for cnt in row_data.values():
                    result_list.append(cnt)

            operation = Variable.get("operation")
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
    dag_id='task',
    catchup=False,
    start_date=datetime(2024, 9, 19),
    schedule=None,
    tags=['stream_1'],
    default_args=default_args,
) as dag_1:
    sensor_task = FileSensor(task_id='wait_for_file', filepath='/opt/airflow/dags/data/raw_data.txt', poke_interval=10)
    process_raw_task = PythonOperator(task_id='process_raw_task', python_callable=process_raw_fn, outlets=[Dataset('/opt/airflow/dags/data/processed_data.txt')])
    move_to_trash_task = BashOperator(task_id='move_to_trash', bash_command='mv --backup=t /opt/airflow/dags/data/raw_data.txt /opt/airflow/dags/data/trash/',)
    start_new_task = TriggerDagRunOperator(task_id='start_new', trigger_dag_id='task')
    log_info_task = BashOperator(task_id='log_info_task', bash_command='echo {{ ti.xcom_pull(task_ids="process_raw_task") }}')

    sensor_task >> process_raw_task >> [move_to_trash_task, start_new_task, log_info_task]

with DAG(
    dag_id='dataset_dag',
    catchup=False,
    start_date=datetime(2024, 9, 19),
    schedule=[Dataset('/data/processed_data.txt')],
    tags=['stream_2'],
    default_args=default_args,
) as dag_2:
    count_words_total_task = PythonOperator(task_id='count_words_total', python_callable=combine_lines)
                    