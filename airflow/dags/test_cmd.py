from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import logging

logger = logging.getLogger("airflow.task")


# Define the DAG
with DAG(
    dag_id="run_python_on_windows",
    start_date=datetime(2026, 2, 7),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    logger.info("DAG run_python_on_windows has been loaded.")

    # Task: Run Python script via Windows cmd
    run_windows_python = BashOperator(
        task_id="run_windows_python",
        bash_command='/opt/airflow/cmd/cmd.exe /c "python C:\\Users\\Bijesh Kumar\\Documents\\fraud-platform\\starter\\train_fraud_model_hist.py"'
    )