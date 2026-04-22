from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
import os

from markets_core.domain.series import SeriesId

from markets_pipeline.sources.yfinance import YfinanceSource
from markets_pipeline.stores.timescale import TimescaleWarehouse


default_args = {
    "owner": "markets",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

observations_dataset = Dataset("s3://markets-observations")

# Canonical series IDs that the ontology references
SERIES = [
    "yfinance:DX-Y.NYB:close",
    "yfinance:BZ=F:close",
]


def _warehouse() -> TimescaleWarehouse:
    dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    return TimescaleWarehouse(dsn=dsn)


def ingest_yfinance_task(logical_date: str) -> None:
    d = datetime.fromisoformat(logical_date).date()
    src = YfinanceSource()
    wh = _warehouse()
    observations = []
    for canonical in SERIES:
        # canonical is "yfinance:<ticker>:<channel>"; split on first ":" only
        _, rest = canonical.split(":", 1)
        sid = SeriesId(source="yfinance", native_id=rest)
        for obs in src.fetch(series_id=sid, start=d, end=d + timedelta(days=1), as_of=d):
            observations.append(obs)
    wh.upsert_observations(observations)


dag = DAG(
    "ingest_yfinance",
    default_args=default_args,
    description="Ingest daily OHLC from yfinance (DXY, Brent)",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

PythonOperator(
    task_id="ingest_yfinance",
    python_callable=ingest_yfinance_task,
    op_kwargs={"logical_date": "{{ logical_date.isoformat() }}"},
    outlets=[observations_dataset],
    dag=dag,
)
