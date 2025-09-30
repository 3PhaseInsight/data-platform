import datetime
import logging
import os
import time

import yaml
from dask.distributed import Client, get_client
from threephi_framework import DataExtractor
from airflow import DAG
from airflow.operators.python import PythonOperator

PHASE_MEASUREMENTS_READY_PATH = "phase_measurements/raw"

logger = logging.getLogger()

def test_and_time_workflows():
    # Set up dask client
    try:
        get_client()
    except ValueError:
        Client("tcp://dask-scheduler:8786")

    config_file = "test_and_time_workflows.yaml"

    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'configs', config_file), 'r') as file:
        pipeline_config = yaml.safe_load(file)

    meter_id = pipeline_config["get_single_meter"]["id"]

    data_extractor = DataExtractor(phase_measurements_dir=pipeline_config["data_dir_path"])

    """ Testing workflow to get timeseries data """
    workflow = "get_timeseries_data"
    logger.info(f"Start {workflow}")
    start_time = time.time()
    result = data_extractor.v1_get_timeseries_info()
    logger.info(f"""
Timeseries Data:
Earliest: {result['min_timestamp']},
Latest: {result['max_timestamp']},
List of Meter ID Length: {len(result['id_list_of_sms_with_data'])}
""")
    logger.info(f"--- {workflow}: {time.time() - start_time} seconds ---")

    """ Get single meter data """
    workflow = "get_single_meter_data"
    logger.info(f"Start {workflow}")
    start_time = time.time()
    ddf = data_extractor.v1_get_single_meter_data(meter_id)
    logger.info(f"Dask DataFrame columns: {list(ddf.columns)}")
    logger.info(f"Number of partitions: {ddf.npartitions}")
    logger.info(f"Dtypes:\n{ddf.dtypes}")
    row_count = ddf.shape[0].compute()
    logger.info(f"Total number of rows: {row_count}")
    logger.info(f"Describe stats:\n{ddf.describe().compute()}")

    logger.info(f"--- {workflow}: {time.time() - start_time} seconds ---")

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
        dag_id='test_and_time_workflows',
        description='Ingest Timeseries Data from CSV to partitioned parquet file storage',
        default_args=default_args,
        start_date=datetime.datetime.now(),
        catchup=False,
        max_active_runs=1,  # Prevent concurrent runs, protect from DB inconsistencies
        tags=['testing', 'workflows'],
) as dag:

    test_task = PythonOperator(
        task_id='test_workflows',
        python_callable=test_and_time_workflows,
        provide_context=True,
        pool='database_pool',  # Use connection pool if configured
        doc_md="""
        ## Workflows
        # Get Timeseries Info
        # Fetch Data for a single meter and do some calculations
        """
    )

    test_task