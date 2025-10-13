from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dask.distributed import Client, get_client
from threephi_framework import TopologyIngestor

def test_topology():
    ingestor = TopologyIngestor(
        "/opt/airflow/data/lv_topology.csv",
        "/opt/airflow/data/meter_cabinet_connection.csv"
    )
    # query meters for substation
    meters = ingestor.db_connector.get_meters_for_substation(147237)
    print(meters)

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