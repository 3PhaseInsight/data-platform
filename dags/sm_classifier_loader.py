import os
import numpy as np
import matplotlib
matplotlib.use("Agg")
from matplotlib import pyplot as plt
import yaml
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

class Save_SMClassifier(DataApp):

    # TODO: Is this important??
    # Some variables for plotting
    ALLOWED_VARIABLES = {"V", "P14", "P23", "Q12", "Q34"}
    ALLOWED_PHASES = {"L1", "L2", "L3"}

    def __init__(self, config):

        # Set up the config settings from the parent class
        super().__init__(config)

        # Unpack the config
        # self.batch = config.get('Data_batch')
        # self.use_dask = config.get('Use_dask')
        self.data_extractor = DataExtractor()
        self.topology_controller = TopologyController(threephi_db.new_session)
        self.meta_controller = MetaController(threephi_db.new_session)
        self.n_workers = config["Cluster_settings"]["n_workers"]
        self.sm_ids = config.get('sm_ids', "All")
        self.topology_processing_level = config.get('topology_processing_level', None)
        self.overwrite_topology_info = config.get('overwrite_topology_info', False)
        self.overwrite_timeseries_info = config.get('overwrite_timeseries_info', False)
        self.results_dir = f'{self.data_extractor.s3_base}/sm_classifier'
        
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
    
    def _check_correct_setup(self):
        
        # Check and store user settings
        if not (isinstance(self.sm_ids, str) or (isinstance(self.sm_ids, list) and all(isinstance(i, str) for i in self.sm_ids))):
            raise TypeError("sm_ids must be set 'All' or user-specified SM IDs as string or list of strings.")

        if not isinstance(self.run_name, str):
            raise TypeError("run_name must be a string.")

        if sum(self.plot_cfg["SM_selection"].values()) == 0:
            raise ValueError("At least one 'SM_selection' option in the plot_cfg must be set to True.")
        if not set(var.upper() for var in self.plot_cfg["Variable_selection"]).issubset(self.ALLOWED_VARIABLES):
            raise ValueError(f"'Variable_selection' in the plot_cfg contains invalid entries. Allowed entries: {self.ALLOWED_VARIABLES}")
        if not set(self.plot_cfg["Variable_selection"]):
            raise ValueError("At least one variable must be selected in 'Variable_selection' of the plot_cfg.")
        if not set(var.upper() for var in self.plot_cfg["Phase_selection"]).issubset(self.ALLOWED_PHASES):
            raise ValueError(f"'Phase_selection' in the plot_cfg contains invalid entries. Allowed entries: {self.ALLOWED_PHASES}")
        if not set(self.plot_cfg["Phase_selection"]):
            raise ValueError("At least one phase must be selected in 'Phase_selection' of the plot_cfg.")

    def _export_cfg(self) -> dict:
        return {
            "run_name": self.run_name,
            "plot_cfg": self.plot_cfg,
            "no_data_limit": self.no_data_limit,
            "good_data_limit": self.good_data_limit,
            "medium_data_limit": self.medium_data_limit,
            "v_lim": self.v_lim,
            "offset_threshold": self.offset_threshold,
            "cons_period_threshold": self.cons_period_threshold,
            "frozen_range": self.frozen_range,
            "selected_variables": self.selected_variables if self.plot_cfg is not None else None,
            "selected_phases": self.selected_phases if self.plot_cfg is not None else None,
            "phases":  ["l1", "l2", "l3"],
            "variables": ["v", "p14", "p23", "q12", "q34"],
            "topology_processing_level": self.topology_processing_level,
            "overwrite_topology_info": self.overwrite_topology_info,
            "overwrite_timeseries_info": self.overwrite_timeseries_info,
            "max_rec_period": None,
        }

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

        # Determine sm_ids to process
        if self.sm_ids == "All":
            # Create a set of all sm_ids from the topology
            sm_ids = sorted(set(self.topology_controller.get_meters()))
        else:
            sm_ids = self.sm_ids
        
        meter_id = sm_ids[0]
        earlier_result = self.data_extractor.s3_connector.read_json(path = f"{self.results_dir}/sm_classification_{meter_id}.json")


        logging.info(f"Earlier result: {earlier_result}")
        logging.info(f"Now injecting in the meta_controller!")


        self.meta_controller.update_sm_characterization(meter_id = meter_id, data = earlier_result)

        logging.info("Done injecting! Will start loading results now...")

        results = self.meta_controller.get_sm_characterization(meter_id = meter_id)
        logging.info(f"SM {meter_id} classification: {results}")
        
        # Save results to S3
        # self.data_extractor.s3_connector.write_json(path = f"{self.results_dir}/sm_classification_{now}.json", data = results)

        # return results

    

config_file = "sm_classifier_config.yaml"
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'configs', config_file), 'r') as file:
    pipeline_config = yaml.safe_load(file)
    save_sm_classifier = Save_SMClassifier(config=pipeline_config)

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