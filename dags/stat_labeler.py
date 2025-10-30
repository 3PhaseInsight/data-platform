import os
import yaml
import logging
from time import sleep
import numpy as np
from dask.distributed import get_client, Client
from dask import delayed, compute
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dask.distributed import Client, get_client

from threephi_framework import DataApp
from threephi_framework import DataExtractor
import threephi_framework.db.db as threephi_db
from threephi_framework import TopologyController
from threephi_framework.dtu.stat_labeler import _label_meters

class StatLabeler(DataApp):

    # Initialization method which is automatically called when creating an instance of this class
    def __init__(self, config):

        # Set up the config settings from the parent class
        # set up batch, profile_processing_level, result_name, dask client, logger, and data extractor
        super().__init__(config)

        # Set up the local config settings
        self.data_extractor = DataExtractor()
        self.use_ANOVA = config.get('use_ANOVA', True)
        self.label_summerhouse = config.get('label_summerhouse', True)
        self.process_only_sm_with_hp = config.get('process_only_sm_with_hp', False)
        self.n_workers = config["Cluster_settings"]["n_workers"]
        self.sm_ids = config.get('sm_ids', None)
        self.overwrite_existing_results = config.get('overwrite_existing_results', False)
        self.save_meta_results = config.get('save_meta_results', False)
        self.save_plots = config.get('save_plots', False)
        self.filter_data = config.get('filter_data', True)
        self.topology_controller = TopologyController(threephi_db.new_session)
        self.weather_file = "/opt/airflow/data/weather_data.csv"
        self.results_dir = f'{self.data_extractor.s3_base}/stat_labeler_results'
        
        self.thresholds = {"weekly_change": 0.01,
                        "static_days": 0.4,
                        "weekend_ratio": 0.45,
                        "max_bins": 12,
                        "min_bins": 1,
                        "filter_temp": 3,
                        "anova_pvalue": 0.01,}
        
    # Method to update config settings via the method arguments
    def _update_config(self, args):
        for arg_name, arg_value in args:
            if arg_name != 'self' and arg_value is not None:
                setattr(self, arg_name, arg_value)

    def _export_cfg(self) -> dict:
        return {
            "process_only_sm_with_hp": self.process_only_sm_with_hp,
            "overwrite_existing_results": self.overwrite_existing_results,
            "label_summerhouse": self.label_summerhouse,
            "filter_data": self.filter_data,
            "save_plots": self.save_plots,
            "use_ANOVA": self.use_ANOVA,
            "thresholds": dict(self.thresholds),
            "results_dir": str(self.results_dir),
            "weather_file": self.weather_file,
        }

    # Method to check for previous results in earlier results files
    def _check_previous_results(self, earlier_results_paths, sm_id, heat_pump_returns):
        sm_str = str(sm_id)
        for results_path in earlier_results_paths:
            try:
                results_data = self.data_extractor.s3_connector.read_json(results_path)
                if sm_str in results_data:
                    heat_pump_returns[sm_id] = results_data[sm_str]
                    logging.info(f"Label results of {sm_id} already exists in earlier results. Loading existing results.")
                    return heat_pump_returns, False
            except Exception as e:
                logging.warning(f"Could not read {results_path}: {e}")
                continue     
        return heat_pump_returns, True

    # Method to perform statistical labeling of heat pumps in smart meter data
    def stat_label_sm(self,
                           sm_ids: list = None,
                           filter_data: bool = None,
                           label_summerhouse: bool = None,
                           use_ANOVA: bool = None,
                           process_only_sm_with_hp: bool = None,
                           save_plots: bool = None,
                           save_meta_results: bool = None,
                           overwrite_existing_results: bool = None
                           ) -> dict:

        # Overwrite config settings with arguments if provided (allows to dynamically change data app run in pipeline)
        self._update_config(args=locals().items())

        self.sm_ids = [str(sm_id) for sm_id in self.sm_ids]

        # Create folder for results if it does not exist
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)

        # Find earlier results files in the results directory
        earlier_results_paths = [os.path.join(self.results_dir, f) for f in os.listdir(self.results_dir) if "results" in f]


        sm_with_hp = self.topology_controller.get_meters(has_heat_pump=True)

        sm_id_chunks = np.array_split(self.sm_ids, min(len(self.sm_ids), self.n_workers))
        cfg = self._export_cfg()
        delayed_tasks = [delayed(_label_meters)(sm_ids_chunk, sm_with_hp, cfg) for sm_ids_chunk in sm_id_chunks]
        # meta_results_list = compute(*delayed_tasks)

        results = compute(*delayed_tasks)
        meta_results_list = [r[0] for r in results]
        heat_pump_list = [r[1] for r in results]
        heat_pump_returns = [r[2] for r in results]

        meta_results_merged = {k: v for d in meta_results_list for k, v in d.items()}
        heat_pump_merged = {k: v for d in heat_pump_list for k, v in d.items()}
        heat_pump_returns = {k: v for d in heat_pump_returns for k, v in d.items()}
        sleep(1)
        
        # Save results if desired
        # Add timestamp to the filename
        now = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
        if self.save_meta_results:
            self.data_extractor.s3_connector.write_json(path = self.results_dir + f'meta_results_{now}.json', data = meta_results_merged)
            self.data_extractor.s3_connector.write_json(path = self.results_dir + f'heat_pump_results_{now}.json', data = heat_pump_merged)
        else:
            self.data_extractor.s3_connector.write_json(path = self.results_dir + f'heat_pump_results_{now}.json', data = heat_pump_merged)

        return heat_pump_returns
    
    def run(self):
        self.stat_label_sm()


config_file = "stat_labeler_config.yaml"

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'configs', config_file), 'r') as file:
    pipeline_config = yaml.safe_load(file)
    statlabeler = StatLabeler(config=pipeline_config)

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
        dag_id="statlabeler",
        description="Locate and label SM phases with heat pumps using statistical methods",
        default_args=default_args,
        start_date=datetime.now(),
        catchup=False,
        max_active_runs=1,  # Prevent concurrent runs, protect from DB inconsistencies
    ) as dag:
        stat_labeler_task = PythonOperator(
            task_id="Label_phases_of_SMs",
            python_callable=statlabeler.run,
            doc_md="""
            ## Label phases of SMs
            """,
        )

        stat_labeler_task
