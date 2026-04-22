from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
import os

from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_pipeline.classifiers.rule_based import RuleBasedClassifier


default_args = {
    "owner": "markets",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

derived_dataset = Dataset("s3://markets-derived")


def _warehouse() -> TimescaleWarehouse:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL environment variable is required")
    return TimescaleWarehouse(dsn=dsn)


def classify_task(logical_date: str) -> None:
    d = datetime.fromisoformat(logical_date).date()
    wh = _warehouse()
    clf = RuleBasedClassifier(warehouse=wh)
    result = clf.classify(d)
    wh.upsert_regime_state({
        "as_of_date": result.as_of_date,
        "classifier_name": result.classifier_name,
        "classifier_version": result.classifier_version,
        "regime_name": result.regime_name,
        "confidence": result.confidence,
        "trigger_variables": result.trigger_variables,
        "rationale": result.rationale,
        "rationale_detail": result.rationale_detail,
        "ontology_version": result.ontology_version,
    })


dag = DAG(
    "classify",
    default_args=default_args,
    description="Classify market regimes",
    schedule=[derived_dataset],
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

PythonOperator(
    task_id="classify_regimes",
    python_callable=classify_task,
    op_kwargs={"logical_date": "{{ logical_date.isoformat() }}"},
    dag=dag,
)