from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
import os

from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_pipeline.classifiers.rule_based import RuleBasedClassifier

# Default args
default_args = {
    "owner": "markets",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Datasets
derived_dataset = Dataset("s3://markets-derived")

def classify_regimes_task(execution_date):
    """Classify market regimes."""
    db_dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    
    warehouse = TimescaleWarehouse(dsn=db_dsn)
    classifier = RuleBasedClassifier(warehouse=warehouse)
    
    exec_dt = datetime.fromisoformat(execution_date)
    
    # Classify as of this logical date
    regime_states = classifier.classify(as_of=exec_dt.date())
    
    if regime_states:
        warehouse.upsert_regime_states(regime_states)

# Define DAG
dag = DAG(
    "classify",
    default_args=default_args,
    description="Classify market regimes",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

# Tasks
classify_task_op = PythonOperator(
    task_id="classify_regimes",
    python_callable=classify_regimes_task,
    op_kwargs={"execution_date": "{{ execution_date.isoformat() }}"},
    dag=dag,
)