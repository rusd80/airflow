from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pandas as pd
import requests
import urllib2
import urllib.request

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils import timezone

with DAG(
        dag_id="taskcsv_dag",
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        render_template_as_native_obj=True,
        tags=["test"],
) as dag:

    @task
    def task1():
        print("task1")

    sleep_task = TimeDeltaSensor(
        task_id="sleep",
        delta=timedelta(seconds=3),
        mode='reschedule'
    )

    @task(multiple_outputs=True)
    def duration_task():
        context = get_current_context()
        dag_run = context["dag_run"]
        execution_date = dag_run.execution_date
        now = timezone.make_aware(datetime.utcnow())
        duration = now - execution_date
        return {
            "duration": str(duration),
            "start_time": str(dag_run.execution_date),
            "end_time": str(now)
        }


    @task(multiple_outputs=True)
    def download_csv():

        urllib.request.urlretrieve('https://drive.google.com/uc?id=13a2WyLoGxQKXbN_AIjrOogIlQKNe9uPm', "csv.csv")


    (task1() >> sleep_task >> duration_task() >> download_csv())