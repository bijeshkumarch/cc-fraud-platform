from airflow import DAG
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import shutil
import subprocess
from common.redis_client import get_redis
import platform

LOG = LoggingMixin().log


BASE_DIR = os.environ.get("PROJECT_BASE_PATH")
if not BASE_DIR:
    BASE_DIR = "/opt/project"

# -------------------------------
# CONFIG
# -------------------------------
MODEL_DIR = f"{BASE_DIR}/model"
MODEL_HISTORY_DIR = f"{MODEL_DIR}/model_history"
TRAIN_SCRIPT = f"{BASE_DIR}/batch/train_fraud_model.py"

# avoid printing to stdout; use Airflow logger instead





# -------------------------------
# TASK 2: Train model
# -------------------------------
def train_model(**context):
    """
    Runs batch training script.
    Script should output:
      model/fraud_model_<YYYYMMDD>.pkl
    """
    run_date = context["ds_nodash"]
    env = os.environ.copy()
    env["TRAIN_DATE"] = run_date

    LOG.info("Environment for training: %s", env)
    LOG.info("Running training script: %s", TRAIN_SCRIPT)

    LOG.info("Starting training script for date %s", run_date)
    result = subprocess.run(
        ["python", "batch/train_fraud_model.py"],
        env=env,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        if result.stdout:
            LOG.error(result.stdout)
        if result.stderr:
            LOG.error(result.stderr)
        LOG.exception("Model training failed with return code %s", result.returncode)
        raise RuntimeError("Model training failed")

    LOG.info("Model training completed successfully")

# -------------------------------
# UTILITY: Hold Spark job
# -------------------------------
def hold_spark():
    r = get_redis()
    r.set("spark:fraud:hold", "1")
    LOG.info("Spark HOLD enabled in Redis")

# -------------------------------
# TASK 3: Promote model safely
# -------------------------------
def promote_model(**context):
    run_date = context["ds_nodash"]

    os.makedirs(MODEL_HISTORY_DIR, exist_ok=True)

    # Find all models in model/
    models = [
        f for f in os.listdir(MODEL_DIR)
        if f.startswith("fraud_model_") and f.endswith(".pkl")
    ]

    if not models:
        LOG.error("No models found for promotion")
        raise FileNotFoundError("No models found for promotion")

    # Sort by date (YYYYMMDD)
    models.sort()

    latest_model = models[-1]

    # Move all older models to history
    for m in models[:-1]:
        src = os.path.join(MODEL_DIR, m)
        dst = os.path.join(MODEL_HISTORY_DIR, m)
        shutil.move(src, dst)
        LOG.info("Archived old model: %s", m)

    LOG.info("Active model is now: %s", latest_model)


# -------------------------------
# UTILITY: Release Spark job
# -------------------------------
def release_spark():
    r = get_redis()
    r.delete("spark:fraud:hold")
    LOG.info("Spark HOLD released in Redis")




# -------------------------------
# DAG DEFINITION
# -------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email": ["alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="fraud_model_retrain",
    default_args=default_args,
    description="Fraud model retraining with safe promotion",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["fraud", "ml", "retraining"],
) as dag:


    train = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
        provide_context=True
    )

    promote = PythonOperator(
        task_id="promote_model",
        python_callable=promote_model,
        provide_context=True
    )

    hold_spark = PythonOperator(
        task_id="hold_spark_job",
        python_callable=hold_spark
    )

    release_spark = PythonOperator(
        task_id="release_spark_job",
        python_callable=release_spark
    )


    train >> hold_spark >> promote >> release_spark