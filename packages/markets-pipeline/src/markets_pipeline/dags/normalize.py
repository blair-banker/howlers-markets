from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
import os

from markets_pipeline.stores.parquet import ParquetRawStore
from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_core.domain.series import SeriesId

# Default args
default_args = {
    "owner": "markets",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Input/output datasets
raw_data_dataset = Dataset("s3://markets-raw")
observations_dataset = Dataset("s3://markets-observations")

def normalize_task(execution_date):
    """Normalize raw Parquet into TimescaleDB."""
    db_dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    
    raw_store = ParquetRawStore(path="/data/raw")
    warehouse = TimescaleWarehouse(dsn=db_dsn)
    
    # Read raw observations for DGS10
    series_id = SeriesId(source="fred", native_id="DGS10")
    observations = list(raw_store.read(source="fred", series_id=series_id))
    
    if observations:
        warehouse.upsert_observations(observations)

# Define DAG
dag = DAG(
    "normalize",
    default_args=default_args,
    description="Normalize raw data into warehouse",
    schedule_interval=None,  # Triggered by dataset
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

# Tasks
normalize_task_op = PythonOperator(
    task_id="normalize_observations",
    python_callable=normalize_task,
    op_kwargs={"execution_date": "{{ execution_date.isoformat() }}"},
    dag=dag,
)