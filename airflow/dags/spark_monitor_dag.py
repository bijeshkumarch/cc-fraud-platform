from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="spark_streaming_submit",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "streaming"],
) as dag:

    submit_streaming = SparkSubmitOperator(
        task_id="submit_fraud_streaming",
        application="/opt/project/streaming/spark_streaming_fraud.py",
        conn_id="spark_default",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.2",
        name="fraud_streaming_job",
    )
