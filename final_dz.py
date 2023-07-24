from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from helper_one import greet
from helper_one import getFile
from helper_one import handle_egrul_json_zip
from helper_one import handle_api_get_top_skills

default_args = {
    'owner': 'saperov',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='final_home_work',
    description='It is my third home work',
    start_date=datetime(2023, 7, 15),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
    )

    task2 = BashOperator(
        task_id='download_file',        
        bash_command="wget https://ofdata.ru/open-data/download/egrul.json.zip -O /opt/airflow/dags/egrul.json.zip"
    )

    task3 = PythonOperator(
        task_id='handle_zip_file',
        python_callable=handle_egrul_json_zip
    )

    task4 = PythonOperator(
        task_id='handle_api_get_top_skills',
        python_callable=handle_api_get_top_skills
    )

    task1 >> task2 >> task3 >> task4