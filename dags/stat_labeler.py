from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from threephi_framework import StatLabeler
from utils import load_dag_config


def run_stat_labeler():
    with StatLabeler(config=load_dag_config(__file__)) as app:
        app.run()


default_args = {
    "owner": "inilab",
    "retries": 0,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="statlabeler",
    description="Locate and label SM phases with heat pumps using statistical methods",
    default_args=default_args,
    start_date=datetime.now(),
    catchup=False,
    max_active_runs=1,  # Prevent concurrent runs, protect from DB inconsistencies
) as dag:
    PythonOperator(
        task_id="Label_phases_of_SMs",
        python_callable=run_stat_labeler,
        doc_md="""
        ## Label phases of SMs

        Statistical heat-pump (and summerhouse) labeling per smart meter phase.

        Implemented by `threephi_framework.StatLabeler`; configured via
        `configs/stat_labeler_config.yaml` (the config keys must match
        `StatLabelerConfig` exactly).
        """,
    )
