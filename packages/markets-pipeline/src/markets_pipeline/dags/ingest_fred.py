from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
import os

from markets_pipeline.sources.fred import FredSource
from markets_pipeline.stores.parquet import ParquetRawStore
from markets_core.domain.series import SeriesId

# Default args
default_args = {
    "owner": "markets",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Output dataset
raw_data_dataset = Dataset("s3://markets-raw")

def ingest_fred_task(execution_date):
    """Ingest DGS10 from FRED."""
    fred_key = os.environ.get("FRED_API_KEY")
    if not fred_key:
        raise ValueError("FRED_API_KEY not set")
    
    source = FredSource(api_key=fred_key)
    store = ParquetRawStore(path="/data/raw")
    
    # Fetch DGS10 for the given date
    exec_dt = datetime.fromisoformat(execution_date)
    series_id = SeriesId(source="fred", native_id="DGS10")
    
    # Fetch last 60 days to data for this logical date
    start_date = (exec_dt.date() - timedelta(days=60)).isoformat()
    end_date = exec_dt.date().isoformat()
    
    observations = list(source.fetch(
        series_id=series_id,
        start=datetime.fromisoformat(start_date).date(),
        end=datetime.fromisoformat(end_date).date(),
    ))
    
    if observations:
        store.append(
            source="fred",
            series_id=series_id,
            observations=observations,
            as_of=exec_dt.date(),
        )

# Define DAG
dag = DAG(
    "ingest_fred",
    default_args=default_args,
    description="Ingest FRED DGS10 data",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    datasets=[raw_data_dataset],
)

# Tasks
ingest_task = PythonOperator(
    task_id="ingest_dgs10",
    python_callable=ingest_fred_task,
    op_kwargs={"execution_date": "{{ execution_date.isoformat() }}"},
    dag=dag,
)