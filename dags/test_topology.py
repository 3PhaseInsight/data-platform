import logging
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dask.distributed import Client, get_client

import threephi_framework.db.db as threephi_db
from threephi_framework.controllers.topology import TopologyController

def test_topology():
    topology_controller = TopologyController(threephi_db.new_session)
    meters = topology_controller.get_meters_for_substation("14066")
    logging.info(f"Meters: {meters}")

# Default DAG args
default_args = {
    'owner': 'inilab',
    'retries': 0,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define DAG
with DAG(
        dag_id='test_topology',
        description='Test Topology Data',
        default_args=default_args,
        start_date=datetime.now(),
        catchup=False,
        max_active_runs=1,  # Prevent concurrent runs, protect from DB inconsistencies
) as dag:

    test_topology_task = PythonOperator(
        task_id='ingest_topology',
        python_callable=test_topology,
        doc_md="""
        ## Test Topology
        """
    )

    test_topology_task