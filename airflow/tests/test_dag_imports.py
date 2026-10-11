"""Basic DAG-bag validation for an Airflow-enabled CI environment."""

from pathlib import Path

from airflow.models import DagBag


def test_dags_import_without_errors() -> None:
    dags_path = Path(__file__).parents[1] / "dags"
    dag_bag = DagBag(dag_folder=str(dags_path), include_examples=False)

    assert not dag_bag.import_errors
    assert "rrr_daily_pipeline" in dag_bag.dags
    assert "varanasi_daily_pipeline" in dag_bag.dags

