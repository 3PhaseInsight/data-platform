import os
import numpy as np
import matplotlib
# matplotlib.use("Agg")
from matplotlib import pyplot as plt
import yaml
import dask.dataframe as dd
from time import time, sleep
from typing import Union, List
from threephi_framework import DataApp
from threephi_framework import DataExtractor
import threephi_framework.db.db as threephi_db
from dask.distributed import get_client, Client
from dask import delayed, compute
from airflow import DAG
import logging
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from threephi_framework.controllers.topology import TopologyController
from threephi_framework.controllers.meta import MetaController

from dtu.sm_classifier import _meter_evaluation

# For meta controller
import logging
from collections.abc import Callable
from typing import Any
from sqlalchemy.orm import Session

from threephi_framework.resources.meta.meter import MetaMeterResource
from threephi_framework.resources.topology.assets.meter import MeterResource
from threephi_framework.db_connector import DBConnector
from threephi_framework.s3_connector import S3Connector

from threephi_framework.data_extractor.schemas.phase_measurements.v1 import (
    VERSION,
    PhaseMeasurementsCsvSchema,
    PhaseMeasurementsParquetSchema,
)

class Save_SMClassifier(DataApp):

    # TODO: Is this important??
    # Some variables for plotting
    ALLOWED_VARIABLES = {"V", "P14", "P23", "Q12", "Q34"}
    ALLOWED_PHASES = {"L1", "L2", "L3"}

    def __init__(self, config, session_factory: Callable[[], Session]):

        # Set up the config settings from the parent class
        super().__init__(config)

        # Unpack the config
        # self.batch = config.get('Data_batch')
        # self.use_dask = config.get('Use_dask')
        self.data_extractor = DataExtractor()
        self.db_connector = DBConnector()
        self.s3_connector = S3Connector()
        self.topology_controller = TopologyController(threephi_db.new_session)
        self.meta_controller = MetaController(threephi_db.new_session)
        self.n_workers = config["Cluster_settings"]["n_workers"]
        self.sm_ids = config.get('sm_ids', "All")
        self.topology_processing_level = config.get('topology_processing_level', None)
        self.overwrite_topology_info = config.get('overwrite_topology_info', False)
        self.overwrite_timeseries_info = config.get('overwrite_timeseries_info', False)
        self.results_dir = f'{self.data_extractor.s3_base}/sm_classifier'
        self.phase_measurements_csv_schema = PhaseMeasurementsCsvSchema()
        self.phase_measurements_parquet_schema = PhaseMeasurementsParquetSchema()


        # For meta controller
        self._sf = session_factory
        self.meta_meter_resource = MetaMeterResource(self._sf())
        self.topology_meter_resource = MeterResource(self._sf())

        
        # TODO: Check if these are needed
        # self.overwrite_existing_raw_sm_datasets = config.get('overwrite_existing_raw_sm_datasets', False)
        # self.save_results = config.get('save_results', False)

        # Variables for config
        self.run_name = config.get('run_name', str(int(time())))
        self.save_plots = config.get('save_plots', False)
        self.plot_cfg = config.get('plot_cfg', None)
        self.no_data_limit = config.get('no_data_limit', 0.025)  # Limit defining what counts as having "no data" # Fraction of total dataset of the longest recorded period of all SMs
        self.good_data_limit = config.get('good_data_limit', 0.1)  # Limits defining what is "good", "medium", "bad"
        self.medium_data_limit = config.get('medium_data_limit', 0.5)  # Limits defining what is "good", "medium", "bad"
        self.v_lim = config.get('v_lim', 207)  # lower voltage limit for "plausible" measurements
        self.offset_threshold = config.get('offset_threshold', 0.95)  # Fraction of total data which has to be below v_lim to be considered offset data
        self.cons_period_threshold = config.get('cons_period_threshold', 4*24*2)  # Length of constant values from which on a period is considered constant period
        self.frozen_range = config.get('frozen_range', 3*4 )   # Range of consecutive values that have to be identical to consider it as frozen
    
        # TODO: Check if this can eliminate the plugin import issues
        # # Set up the cluster
        # if self.use_dask:
        #     try:
        #         get_client()
        #     except ValueError:
        #         cluster = self._set_up_cluster(config["Cluster"], config["Cluster_settings"])
        #         client = Client(cluster)
        
        # Configure the logger if not already configured
        if not logging.getLogger().hasHandlers():
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


    # Method to update config settings via the method arguments
    def _update_config(self, args):
        for arg_name, arg_value in args:
            if arg_name != 'self' and arg_value is not None:
                setattr(self, arg_name, arg_value)

    def save_sm_classification(self, sm_ids: Union[str, List[str]] = None,
                              topology_processing_level: str = None,
                              overwrite_existing_raw_sm_datasets: bool = None,
                              overwrite_topology_info: bool = None,
                              overwrite_timeseries_info: bool = None,
                              run_name: str = None,
                              save_results: bool = None,
                              save_plots: bool = None,
                              plot_cfg: dict = None) -> tuple:

        # Overwrite config settings with arguments if provided (allows to dynamically change data app run in pipeline)
        self._update_config(args=locals().items())

        # # Determine sm_ids to process
        # if self.sm_ids == "All":
        #     # Create a set of all sm_ids from the topology
        #     sm_ids = sorted(set(self.topology_controller.get_meters()))
        # else:
        #     sm_ids = self.sm_ids
        
        # for meter_id in sm_ids:
        #     logging.info(f"Processing SM ID: {meter_id}. Is meter_id an integer? {isinstance(meter_id, int)}")

        #     results = self.meta_controller.get_sm_characterization(meter_id = meter_id)
        #     logging.info(f"SM {meter_id} classification: {results}")


        def fill_in_postgres():
            import datetime as dt
            import uuid
            import glob


            csv_path = "/opt/airflow/data"
            csv_file_pattern = "phase_measurements_*.csv"

            csv_ts_col = self.phase_measurements_csv_schema.timestamp_col
            parquet_ts_col = self.phase_measurements_parquet_schema.timestamp_col
            parquet_meter_col = self.phase_measurements_parquet_schema.meter_col

            csv_path_pattern = os.path.join(csv_path, csv_file_pattern)
            logging.info(f"Reading CSVs from {csv_path_pattern}")
            csv_files = glob.glob(csv_path_pattern)
            run_id = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8]
            for csv_path in csv_files:
                logging.info(f"Processing {csv_path}")
                # ---------------------------------
                # Create batch row for file
                # ---------------------------------
                batch_id = self.db_connector.insert_batch(csv_path, run_id)

                # ---------------------------------
                # Read in CSV,
                # normalize timestamp to UTC,
                # rename timestamp column,
                # type important cols,
                # drop rows with invalid timestamps,
                # drop rows without meter_number
                # ---------------------------------

                dask_df = dd.read_csv(csv_path, parse_dates=[csv_ts_col])
                dask_df[csv_ts_col] = dd.to_datetime(dask_df[csv_ts_col], utc=True, errors="coerce")
                dask_df = dask_df.rename(columns={csv_ts_col: parquet_ts_col})
                dask_df[parquet_meter_col] = dask_df[parquet_meter_col].astype("string")
                dask_df = dask_df.dropna(subset=[parquet_ts_col])
                dask_df = dask_df.dropna(subset=[parquet_meter_col])

                # ---------------------------------
                # Get stats for meter inventory table
                # ---------------------------------
                stats_workflow = f"{csv_path}_stats"
                if not self.db_connector.is_workflow_completed(stats_workflow):
                    self.db_connector.start_workflow(stats_workflow)

                    logging.info("Computing meter inventory stats.")
                    stats_ddf = dask_df[[parquet_meter_col, parquet_ts_col]]
                    agg_ddf = stats_ddf.groupby(parquet_meter_col)[parquet_ts_col].agg(["min", "max", "count"])

                    agg_pdf = agg_ddf.compute()
                    agg_pdf = agg_pdf.rename(columns={"min": "first_seen", "max": "last_seen", "count": "total_rows"})
                    agg_pdf = agg_pdf.reset_index(names=["id"])
                    self.db_connector.upsert_meter_stats(agg_pdf)
                    self.db_connector.complete_workflow(stats_workflow)
                    logging.info("Successfully computed and stored stats.")
        
        
        def get_timeseries_data():
            
            timeseries = self.data_extractor.v1_get_timeseries_info()

            """
            Function for extracting some information from the timeseries data.

            Information:
                - min_timestamp: earliest SM measurement timestamp
                - max_timestamp: last SM measurement timestamp
                - id_list_of_sms_with_data: list of meter IDS that we have data for
            """
            logging.info(f"min_timestamp: {timeseries['min_timestamp']}")
            logging.info(f"max_timestamp: {timeseries['max_timestamp']}")
            logging.info(f"Number of SMs with data: {len(timeseries['id_list_of_sms_with_data'])}")
        
        def get_timeseries_data_2():
            self.data_extractor._get_timeseries_info_db()
            sm_with_data = self.data_extractor.id_list_of_sms_with_data
            logging.info(f"Number of SMs with data (from DB): {len(sm_with_data)}")
            logging.info(f"Some SM IDs with data: {sm_with_data[:10]}")

            # Ensure that sm_with_data has strings as elements
            sm_with_data = [str(sm_id) for sm_id in sm_with_data]

            for sm_id in self.sm_ids:
                logging.info(f"SM_id {sm_id} exist in data: {sm_id in sm_with_data}")

        def load_classification_results():
            for meter_id in self.sm_ids:
                results = self.meta_controller.get_sm_characterization(meter_id = meter_id)
                logging.info(f"SM {meter_id} classification: {results}")

        load_classification_results()
    

config_file = "sm_classifier_config.yaml"
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'configs', config_file), 'r') as file:
    pipeline_config = yaml.safe_load(file)
    save_sm_classifier = Save_SMClassifier(config=pipeline_config, session_factory=threephi_db.new_session)

    # Default DAG args
    default_args = {
        "owner": "inilab",
        "retries": 0,
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    }

    # Define DAG
    with DAG(
        dag_id="save_sm_classifier",
        description="SAVE Smart Meters Classification based on topology and data quality",
        default_args=default_args,
        start_date=datetime.now(),
        catchup=False,
        max_active_runs=1,  # Prevent concurrent runs, protect from DB inconsistencies
    ) as dag:
        save_sm_classifier_task = PythonOperator(
            task_id="Save_SM_Classification",
            python_callable=save_sm_classifier.save_sm_classification,
            doc_md="""
            ## SAVE Smart Meters Classification DAG
            """,
        )

        save_sm_classifier_task