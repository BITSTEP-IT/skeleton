from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# 간단한 테스트 함수
def print_hello():
    print("✅ Hello from Airflow DAG!")

# 기본 DAG 설정값
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG 정의
with DAG(
    dag_id='test_hello_dag',
    default_args=default_args,
    description='A simple test DAG to verify Airflow setup',
    schedule_interval='@daily',
    catchup=False,
    tags=['test']
) as dag:

    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello
    )

    hello_task