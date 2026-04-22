from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
import os

from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_pipeline.transforms.zscore import ZScoreTransform

# Default args
default_args = {
    "owner": "markets",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Datasets
observations_dataset = Dataset("s3://markets-observations")
derived_dataset = Dataset("s3://markets-derived")

def derive_zscores_task(execution_date):
    """Derive z-scores from observations."""
    db_dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    
    warehouse = TimescaleWarehouse(dsn=db_dsn)
    transform = ZScoreTransform(warehouse=warehouse, window_days=63)
    
    exec_dt = datetime.fromisoformat(execution_date)
    
    # Compute z-scores for DGS10
    zscores = transform.compute(series_id="fred:DGS10", as_of=exec_dt.date())
    
    if zscores:
        warehouse.upsert_zscores(zscores)

# Define DAG
dag = DAG(
    "derive",
    default_args=default_args,
    description="Derive analytical views",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

# Tasks
derive_task_op = PythonOperator(
    task_id="compute_zscores",
    python_callable=derive_zscores_task,
    op_kwargs={"execution_date": "{{ execution_date.isoformat() }}"},
    dag=dag,
)