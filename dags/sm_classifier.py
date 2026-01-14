import pandas as pd
import os
import sys
import numpy as np
import logging
import matplotlib
matplotlib.use("Agg")
from matplotlib import pyplot as plt
import yaml
import json
from tqdm import tqdm
from time import time, sleep
from datetime import datetime
from typing import Union, List
import importlib

from dask.distributed import get_client, Client
from dask import delayed, compute
from threephi_framework.controllers.meta import MetaController
# from threephi_framework import TopologyController
from threephi_framework import DataApp
from threephi_framework import DataExtractor
import threephi_framework.db.db as threephi_db
from airflow import DAG
from dtu.sm_classifier import meter_evaluation

from airflow.providers.standard.operators.python import PythonOperator








class SMClassifier(DataApp):

    # # Some variables for plotting
    # DIR_DATA_APP = os.path.dirname(os.path.abspath(__file__))
    # ALLOWED_VARIABLES = {"V", "P14", "P23", "Q12", "Q34"}
    # ALLOWED_PHASES = {"L1", "L2", "L3"}

    def __init__(self, config):

        # Set up the config settings from the parent class
        # set up batch, profile_processing_level, result_name, dask client, logger, and data extractor
        super().__init__(config)

        # Unpack the config
        # self.batch = config.get('Data_batch')
        # self.use_dask = config.get('Use_dask')
        self.n_workers = config["Cluster_settings"]["n_workers"]
        self.sm_ids = config.get('sm_ids', "All")
        self.topology_processing_level = config.get('topology_processing_level', None)
        self.overwrite_existing_raw_sm_datasets = config.get('overwrite_existing_raw_sm_datasets', False)
        self.overwrite_topology_info = config.get('overwrite_topology_info', False)
        self.overwrite_timeseries_info = config.get('overwrite_timeseries_info', False)
        self.run_name = config.get('run_name', str(int(time())))
        self.save_results = config.get('save_results', False)
        self.save_plots = config.get('save_plots', False)
        self.plot_cfg = config.get('plot_cfg', None)
        self.no_data_limit = config.get('no_data_limit', 0.025)  # Limit defining what counts as having "no data" # Fraction of total dataset of the longest recorded period of all SMs
        self.good_data_limit = config.get('good_data_limit', 0.1)  # Limits defining what is "good", "medium", "bad"
        self.medium_data_limit = config.get('medium_data_limit', 0.5)  # Limits defining what is "good", "medium", "bad"
        self.v_lim = config.get('v_lim', 207)  # lower voltage limit for "plausible" measurements
        self.offset_threshold = config.get('offset_threshold', 0.95)  # Fraction of total data which has to be below v_lim to be considered offset data
        self.cons_period_threshold = config.get('cons_period_threshold', 4*24*2)  # Length of constant values from which on a period is considered constant period
        self.frozen_range = config.get('frozen_range', 3*4 )   # Range of consecutive values that have to be identical to consider it as frozen
        
        self.results_dir = f'{self.data_extractor.s3_base}/sm_classifier'
        # self.topology_controller = TopologyController(threephi_db.new_session)

        # Store DataExtractor
        self.DataExtractor = DataExtractor()

        # Placeholder for some class attributes that will be assigned later
        self.selected_variables = None
        self.selected_phases = None
        self.sm_selection = None
        self.max_rec_period = None
        self.SM_topology_mapping = None
        self.dir_of_current_run_results = None

        # Configure the logger if not already configured
        if not logging.getLogger().hasHandlers():
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        # # Set up the cluster
        # if self.use_dask:
        #     try:
        #         get_client()
        #     except ValueError:
        #         cluster = self._set_up_cluster(config["Cluster"], config["Cluster_settings"])
        #         client = Client(cluster)


        # Define data type dicts for data loading
        # self.meter_and_cabinet_dtypes = {self.meter_number_col: pd.StringDtype(),
        #                                  self.cabinet_col: pd.StringDtype(),
        #                                  **({"delivery_point_id": pd.StringDtype(),
        #                                      "lv_feeder": pd.StringDtype(),
        #                                      "has_heat_pump": pd.BooleanDtype(),
        #                                      "has_solar_panel": pd.BooleanDtype(),
        #                                      "capacity_solar_panel": pd.Float32Dtype(),
        #                                      "service_fuse_size": pd.Float32Dtype()}
        #                                     if self.batch == "second_batch" or "third_batch" else {})}



    def _save_sm_plot(self, sm_id, sm_df, result_summary):

        # Determine under which directories the plot has to be saved according to user settings and data characteristics
        dirs_to_save = []

        if self.plot_cfg["SM_selection"]["All_(with_dataset_containing_data)"] and sm_id in result_summary["SMs_with_dataset_containing_data"]:
            dirs_to_save.append(os.path.join("Results", f"{self.run_name}", "Plots", "All_SMs_with_dataset_containing_data"))

        if self.plot_cfg["SM_selection"]["With_only_good_data_quality"] and sm_id in result_summary["SMs_with_only_good_data_quality"]:
            dirs_to_save.append(os.path.join("Results", f"{self.run_name}", "Plots", "SMs_with_only_good_data_quality"))

        if self.plot_cfg["SM_selection"]["With_any_medium_or_bad_data_quality"] and sm_id in result_summary["SMs_with_any_medium_or_bad_data_quality"]:
            dirs_to_save.append(os.path.join("Results", f"{self.run_name}", "Plots", "SMs_with_any_medium_or_bad_data_quality"))

        if self.plot_cfg["SM_selection"]["With_1-phase_connection"] and (sm_id in result_summary["SMs_with_1-phase_connection"]):
            dirs_to_save.append(os.path.join("Results", f"{self.run_name}", "Plots", "SMs_with_1-phase_connection"))

        if self.plot_cfg["SM_selection"]["With_2-phase_connection"] and (sm_id in result_summary["SMs_with_2-phase_connection"]):
            dirs_to_save.append(os.path.join("Results", f"{self.run_name}", "Plots", "SMs_with_2-phase_connection"))

        if self.plot_cfg["SM_selection"]["With_connection_error"] and sm_id in result_summary["SMs_with_connection_error"]:
            dirs_to_save.append(os.path.join("Results", f"{self.run_name}", "Plots", "SMs_with_connection_error"))

        if self.plot_cfg["SM_selection"]["With_on_off_switch"] and sm_id in result_summary["SMs_with_on_off_switch"]:
            dirs_to_save.append(os.path.join("Results", f"{self.run_name}", "Plots", "SMs_with_on_off_switch"))

        # If plot belongs in None of the categories, skip plotting
        if not dirs_to_save:
            return

        # Determine the rows needed for the selected variables and phases
        rows = []
        if "V" in self.selected_variables:
            rows.extend([f"{self.voltage_col}{phase}" for phase in self.selected_phases])
        if any(var in self.selected_variables for var in ["P14", "P23"]):
            rows.extend([f"{self.active_power_p14_col[:13]}{phase}" for phase in self.selected_phases])
        if any(var in self.selected_variables for var in ["Q12", "Q34"]):
            rows.extend([f"{self.reactive_power_q12_col[:15]}{phase}" for phase in self.selected_phases])

        # Set up the figure with the correct number of subplots
        n_rows = len(rows)
        fig, axes = plt.subplots(n_rows, 1, sharex=True, figsize=(10, 2 * n_rows))
        if n_rows == 1:
            axes = [axes]  # Ensure axes is iterable for a single row

        # Plot each variable for each selected phase
        row_index = 0
        for row in rows:
            phase = row.split("_")[-1]

            # Set grid for each subplot
            axes[row_index].grid(True, which='both', axis='both', linestyle='--', linewidth=0.5)

            # Plot voltage if selected
            if row.startswith(self.voltage_col) and "V" in self.selected_variables:
                col_name = f"{self.voltage_col}{phase}_{sm_id}"
                axes[row_index].plot(sm_df[col_name], label=col_name)
                axes[row_index].set_ylabel("V [V]")
                axes[row_index].set_title(f"Voltage - Phase {phase.upper()}")
                # Set x-axis limits to data range
                axes[row_index].set_xlim(sm_df.index.min(), sm_df.index.max())

            # Plot active power if selected (P14 and P23 together in the same subplot)
            elif row.startswith(self.active_power_p14_col[:13]) and any(var in self.selected_variables for var in ["P14", "P23"]):
                if "P23" in self.selected_variables:
                    col_name_p23 = f"{self.active_power_p23_col}{phase}_{sm_id}"
                    axes[row_index].plot(-sm_df[col_name_p23], label="Production")
                if "P14" in self.selected_variables:
                    col_name_p14 = f"{self.active_power_p14_col}{phase}_{sm_id}"
                    axes[row_index].plot(sm_df[col_name_p14], label="Consumption")
                axes[row_index].set_ylabel("P [W]")
                axes[row_index].set_title(f"Active Power - Phase {phase.upper()}")
                # Set legend only if both P14 and P23 are present
                if "P14" in self.selected_variables and "P23" in self.selected_variables:
                    axes[row_index].legend(loc="upper right")
                # Set x-axis limits to data range
                axes[row_index].set_xlim(sm_df.index.min(), sm_df.index.max())

            # Plot reactive power if selected (Q12 and Q34 together in the same subplot)
            elif row.startswith(self.reactive_power_q12_col[:15]) and any(var in self.selected_variables for var in ["Q12", "Q34"]):
                if "Q12" in self.selected_variables:
                    col_name_q12 = f"{self.reactive_power_q12_col}{phase}_{sm_id}"
                    axes[row_index].plot(sm_df[col_name_q12], label="Inductive")
                if "Q34" in self.selected_variables:
                    col_name_q34 = f"{self.reactive_power_q34_col}{phase}_{sm_id}"
                    axes[row_index].plot(-sm_df[col_name_q34], label="Capacitive")
                axes[row_index].set_ylabel("Q [Var]")
                axes[row_index].set_title(f"Reactive Power - Phase {phase.upper()}")
                # Set legend only if both Q12 and Q34 are present
                if "Q12" in self.selected_variables and "Q34" in self.selected_variables:
                    axes[row_index].legend(loc="upper right")
                # Set x-axis limits to data range
                axes[row_index].set_xlim(sm_df.index.min(), sm_df.index.max())

            # Increment row index
            row_index += 1

        # Set the title
        fig.text(0.5, 0.98, f"Smart Meter Data for {sm_id}", ha='center', fontsize=16, fontweight='bold')

        # Adjust layout to prevent overlap
        plt.tight_layout(rect=(0, 0, 1, 0.98))

        # Save the plot to all applicable directories
        for d in dirs_to_save:
            _dir = os.path.join(self.dir_of_current_run_results, d)
            os.makedirs(_dir, exist_ok=True)
            plt.savefig(os.path.join(_dir, f'SM_{sm_id}_plot.svg'))

        plt.close(fig)

    # Method to update config settings via the method arguments
    def _update_config(self, args):
        for arg_name, arg_value in args:
            if arg_name != 'self' and arg_value is not None:
                setattr(self, arg_name, arg_value)

    def classify_smart_meters(self, sm_ids: Union[str, List[str]] = None,
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

        # # Check and store user settings
        # # Check sm_ids
        # if not (isinstance(self.sm_ids, str) or (isinstance(self.sm_ids, list) and all(isinstance(i, str) for i in self.sm_ids))):
        #     raise TypeError("sm_ids must be set 'All' or user-specified SM IDs as string or list of strings.")
        # # Check run_name
        # if not isinstance(self.run_name, str):
        #     raise TypeError("run_name must be a string.")
        
        # if sum(self.plot_cfg["SM_selection"].values()) == 0:
        #     raise ValueError("At least one 'SM_selection' option in the plot_cfg must be set to True.")
        # if not set(var.upper() for var in self.plot_cfg["Variable_selection"]).issubset(self.ALLOWED_VARIABLES):
        #     raise ValueError(f"'Variable_selection' in the plot_cfg contains invalid entries. Allowed entries: {self.ALLOWED_VARIABLES}")
        # if not set(self.plot_cfg["Variable_selection"]):
        #     raise ValueError("At least one variable must be selected in 'Variable_selection' of the plot_cfg.")
        # if not set(var.upper() for var in self.plot_cfg["Phase_selection"]).issubset(self.ALLOWED_PHASES):
        #     raise ValueError(f"'Phase_selection' in the plot_cfg contains invalid entries. Allowed entries: {self.ALLOWED_PHASES}")
        # if not set(self.plot_cfg["Phase_selection"]):
        #     raise ValueError("At least one phase must be selected in 'Phase_selection' of the plot_cfg.")


        # if os.path.exists(self.dir_of_current_run_results):

        #     logging.info(f"SM classification run {self.run_name} exists. Loading and returning existing files...")

        #     with open(os.path.join(self.dir_of_current_run_results, f'{self.run_name}_SM_characterization.json'), 'r') as f:
        #         sm_characterization = json.load(f)

        #     with open(os.path.join(self.dir_of_current_run_results, f'{self.run_name}_SM_classification.json'), 'r') as f:
        #         sm_classification = json.load(f)

        #     return sm_characterization, sm_classification

        # logging.info(f"Classifying Smart Meters based on {self.topology_processing_level} topology")


        # Define list of SM IDs to be evaluated based on user settings
        if self.sm_ids == "All":

            # TODO: list all SMs
            # Earlier this was done by sorting MeterAndCabinet SMs as well as Timeseries SMs
            pass
        else:
            SM_IDs = self.sm_ids

    
        ## DASK SETUP ##


        sm_id_chunks = np.array_split(SM_IDs, min(len(SM_IDs), self.n_workers))
        delayed_tasks = [delayed(meter_evaluation)(sm_ids_chunk) for sm_ids_chunk in sm_id_chunks]
        results_list = compute(*delayed_tasks)

        sm_classification_list = [res for res in results_list]

        sm_classification = {k: v for d in sm_classification_list for k, v in d.items()}
        sleep(1)

        # Save detailed and summarized SM classification results if selected
        if self.save_results:
            now = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
            self.data_extractor.s3_connector.write_json(path = f"{self.results_dir}/sm_characterization_{now}.json", data = sm_classification)


        return sm_classification


    # Run method for Airflow DAG    
    def run(self):
        self.classify_smart_meters()

config_file = "sm_classifier_config.yaml"
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'configs', config_file), 'r') as file:
    pipeline_config = yaml.safe_load(file)
    sm_classifier = SMClassifier(config=pipeline_config)

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
        dag_id="sm_classifier",
        description="Classify Smart Meters based on topology and data quality",
        default_args=default_args,
        start_date=datetime.now(),
        catchup=False,
        max_active_runs=1,  # Prevent concurrent runs, protect from DB inconsistencies
    ) as dag:
        sm_classifier_task = PythonOperator(
            task_id="Classify_Smart_Meters",
            python_callable=sm_classifier.run,
            doc_md="""
            ## Classify Smart Meters DAG
            """,
        )

        sm_classifier_task