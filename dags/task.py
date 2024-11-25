from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pandas as pd
import requests


# Function to download the CSV file
def download_csv(**kwargs):
    url = 'https://drive.google.com/uc?id=13a2WyLoGxQKXbN_AIjrOogIlQKNe9uPm'  # Replace with your CSV URL
    response = requests.get(url)
    file_path = '/tmp/downloaded_file.csv'

    with open(file_path, 'wb') as file:
        file.write(response.content)

    return file_path


# Function to process the CSV file using PySpark
def process_csv(file_path):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, avg

    # Initialize Spark session
    spark = SparkSession.builder
    .appName("CSV Processing")
    .getOrCreate()


# Read the CSV file into a DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Select only numeric columns
numeric_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, (DoubleType, IntegerType))]

# Calculate average for each numeric column
averages = {}
for col_name in numeric_cols:
    avg_value = df.select(avg(col(col_name))).first()[0]
    averages[col_name] = avg_value

print("Averages:", averages)

# Stop the Spark session
spark.stop()

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

with DAG('csv_processing_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    download_task = PythonOperator(
        task_id='download_csv',
        python_callable=download_csv,
        provide_context=True,
    )

    process_task = PythonOperator(
        task_id='process_csv',
        python_callable=process_csv,
        op_kwargs={'file_path': '/tmp/downloaded_file.csv'},
    )

    download_task >> process_task