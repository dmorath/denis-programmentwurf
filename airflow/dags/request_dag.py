import pendulum
import requests

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

def request_google():
    resp=requests.get("https://www.google.de")
    print(resp.status_code)

with DAG(
    dag_id="request_dag",
    start_date=pendulum.datetime(2016, 1, 1),
    schedule="@daily",
    default_args={"retries": 2},
):
    op = PythonOperator(task_id="get_google", python_callable=request_google)