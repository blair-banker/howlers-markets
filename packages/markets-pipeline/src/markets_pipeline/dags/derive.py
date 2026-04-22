from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
import os

from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_pipeline.transforms.zscore import ZScoreTransform
from markets_pipeline.transforms.yoy_change import YoYChangeTransform
from markets_pipeline.transforms.trend import TrendTransform


default_args = {
    "owner": "markets",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

observations_dataset = Dataset("s3://markets-observations")
derived_dataset = Dataset("s3://markets-derived")


def _warehouse() -> TimescaleWarehouse:
    dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    return TimescaleWarehouse(dsn=dsn)


def derive_all_task(logical_date: str) -> None:
    d = datetime.fromisoformat(logical_date).date()
    wh = _warehouse()
    ZScoreTransform(wh).compute(d)
    YoYChangeTransform(wh).compute(d)
    TrendTransform(wh).compute(d)


dag = DAG(
    "derive",
    default_args=default_args,
    description="Derive analytical views (z-score, YoY, trend)",
    schedule=[observations_dataset],
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

PythonOperator(
    task_id="derive_all",
    python_callable=derive_all_task,
    op_kwargs={"logical_date": "{{ logical_date.isoformat() }}"},
    outlets=[derived_dataset],
    dag=dag,
)