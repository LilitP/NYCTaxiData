from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

spark_master = "spark://spark:7077"
spark_app_name = "spark_submit_NYC_taxi_data"
file_path_green = "/usr/local/spark/resources/data/Green/"
file_path_yellow="/usr/local/spark/resources/data/Yeloow/"

now = datetime.now()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="NYC_Taxi_Rides_Mountly", 
        description="This DAG runs a simple Pyspark app.",
        default_args=default_args, 
        start_date=datetime(2023, 1, 24),
        schedule_interval='@monthly'
    )
    
start = DummyOperator(task_id="start", dag=dag)

spark_job = SparkSubmitOperator(
    task_id="spark_job",
    application="/usr/local/spark/app/NYC_taxi_data_spark.py",
    name=spark_app_name,
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[file_path_green,file_path_yellow],
    dag=dag)
    
        
end = DummyOperator(task_id="end", dag=dag)

start >> spark_job >> end