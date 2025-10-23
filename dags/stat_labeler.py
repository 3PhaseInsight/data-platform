import os
import sys
import json
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error
from tqdm import tqdm
import pandas as pd
import yaml
import logging
from time import sleep
import numpy as np
import importlib
from dask.distributed import get_client, Client
from dask import delayed, compute
import matplotlib.pyplot as plt
from scipy.stats import truncnorm
from statsmodels.formula.api import ols
from statsmodels.stats.anova import anova_lm
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dask.distributed import Client, get_client

from threephi_framework import DataApp
from threephi_framework import DataExtractor
import threephi_framework.db.db as threephi_db
from threephi_framework import TypologyController

class StatLabeler(DataApp):

    # Initialization method which is automatically called when creating an instance of this class
    def __init__(self, config):

        # Set up the config settings from the parent class
        # set up batch, profile_processing_level, result_name, dask client, logger, and data extractor
        super().__init__(config)

        # Set up the local config settings
        self.use_ANOVA = config.get('use_ANOVA', True)
        self.label_summerhouse = config.get('label_summerhouse', True)
        self.process_only_sm_with_hp = config.get('process_only_sm_with_hp', False)
        self.use_dask = config.get("Use_dask")
        self.save_results = config.get('save_results', True)
        self.n_workers = config["Cluster_settings"]["n_workers"]
        self.tc_cfg = {**config["TopologyCleaner"], "Data_batch": config["Data_batch"], "Use_dask": config["Use_dask"]}
        self.sm_ids = config.get('sm_ids', None)
        self.overwrite_existing_results = config.get('overwrite_existing_results', False)
        self.save_meta_results = config.get('save_meta_results', False)
        self.save_plots = config.get('save_plots', False)
        self.filter_data = config.get('filter_data', True)
        self.topology_controller = TypologyController(threephi_db.new_session)
        self.weather_file = "/opt/airflow/data/weather_data.csv"
        self.results_dir = "/opt/airflow/data/stat_labeler/"
        
        
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

    # Method to label a smart meter as summerhouse or not
    def _label_summerhouse(self, sm_df, sm_id, meta_results):

        sm_df_filtered = self._filter_data(sm_df)

        # Identify days with static consumption (consumer is not active)
        days_with_static_con = sm_df[~sm_df.index.isin(sm_df_filtered.index)]

        # Identify consumption on weekends
        weekend_mask = sm_df.index.weekday >= 5  # 5=Saturday, 6=Sunday
        weekend_consumption = sm_df[weekend_mask].sum().sum()

        # Identify total consumption
        total_consumption = sm_df.sum().sum()

        # Identify ratio of weekend consumption to normal days
        normal_day_consumption = total_consumption - weekend_consumption
        weekend_ratio = weekend_consumption / normal_day_consumption if normal_day_consumption > 0 else float('inf')

        # If the static dates exceed more than 40% of the total dates, 
        # or if the ratio of weekend consumption to normal days are above 45%,
        # then this might be a summerhouse
        if  len(days_with_static_con) > self.thresholds['static_days'] * len(sm_df_filtered) or \
            weekend_ratio > self.thresholds['weekend_ratio'] if normal_day_consumption > 0 else False:

            # If so, then let us also find the consumption during holidays to further verify
            # Holidays: 16 october to 20 october, 18 december to 3 january, 12 february to 16 february, 25 march to 29 march
            holiday_mask = ((sm_df.index.month == 10) & (sm_df.index.day >= 16) & (sm_df.index.day <= 20)) | \
                        ((sm_df.index.month == 12) & (sm_df.index.day >= 18) & (sm_df.index.day <= 23)) | \
                        ((sm_df.index.month == 12) & (sm_df.index.day >= 25) & (sm_df.index.day <= 29)) | \
                        ((sm_df.index.month == 1) & (sm_df.index.day <= 3)) | \
                        ((sm_df.index.month == 2) & (sm_df.index.day >= 12) & (sm_df.index.day <= 16)) | \
                        ((sm_df.index.month == 3) & (sm_df.index.day >= 25) & (sm_df.index.day <= 29)) | \
                        ((sm_df.index.month == 4) & (sm_df.index.day == 26))
            holiday_consumption = sm_df[holiday_mask].sum().sum()
            
            #proportion of holiday consumption with total consumption
            holiday_proportion = holiday_consumption / total_consumption if total_consumption > 0 else 0
            weekend_and_holiday_proportion = (weekend_consumption + holiday_consumption) / total_consumption if total_consumption > 0 else 0

            meta_results[sm_id]["Summerhouse_info"] = f"Smart meter {sm_id} is likely a summerhouse"
            meta_results[sm_id]["Summerhouse_stats"].append(f'Static_Timestamps: {len(days_with_static_con)}')
            meta_results[sm_id]["Summerhouse_stats"].append(f'Total_Timestamps: {len(sm_df_filtered)}')
            meta_results[sm_id]["Summerhouse_stats"].append(f'Weekend_Consumption: {weekend_consumption} Wh')
            meta_results[sm_id]["Summerhouse_stats"].append(f'Holiday_Consumption: {holiday_consumption} Wh')
            meta_results[sm_id]["Summerhouse_stats"].append(f'Total_Consumption: {total_consumption} Wh')
            meta_results[sm_id]["Summerhouse_stats"].append(f'Normal_Day_Consumption: {normal_day_consumption} Wh')
            meta_results[sm_id]["Summerhouse_stats"].append(f'Weekend_Ratio: {weekend_ratio:.2%}')
            meta_results[sm_id]["Summerhouse_stats"].append(f'Holiday_Proportion: {holiday_proportion:.2%}')
            meta_results[sm_id]["Summerhouse_stats"].append(f'Weekend_and_Holiday_Proportion: {weekend_and_holiday_proportion:.2%}')

            return meta_results
            
        else:
            meta_results[sm_id]["Summerhouse_info"] = f"Smart meter {sm_id} is likely not a summerhouse"
            
            return meta_results

    # Method to plot the smart meter data
    def _plot_sm(self, sm_id, sm_df, savedir, filename=None):
    
        plt.clf() # Clear any previous plot
        
        for col in sm_df.columns: # Plot the different active power columns
            plt.plot(sm_df.index, sm_df[col], label=col)

        # Add labels and title and save
        plt.xlabel("Time")
        plt.ylabel("Active Power P14")
        plt.title(f"Smart Meter {sm_id} Active power" + f"( {filename})" if filename else "")
        plt.legend()
        plt.savefig(os.path.join(savedir, f"sm_{sm_id}" + f"_{filename}.png" if filename else ".png"))

    # Method to get the base load and temperature bins
    def _get_base_load_and_temperature_bins(self, sm_df):
        
        # Assume the base load is the minimum load of a specific temperature bin
        # Find this temperature bin for each phase
        max_bins = self.thresholds['max_bins']
        min_bins = self.thresholds['min_bins']
        n_bins = max_bins
        valid_bins_found = False

        # Find the temperature bins such that all 24 hours of the day are represented in the lowest temperature bin
        while n_bins >= min_bins and not valid_bins_found:
            try:
                # Create temperature bins
                sm_df["T_bin"] = pd.qcut(sm_df["T"], n_bins, labels=range(n_bins))

                tmin_list = []
                valid_bins_found = True  # assume success until proven otherwise

                for phase in range(3):
                    phase_col = sm_df.columns[phase]
                    grouped_means = sm_df[phase_col].groupby(sm_df["T_bin"], observed = False).mean()
                    tmin = grouped_means.idxmin()

                    # Check all 24 hours exist in this tmin bin
                    missing_hours = [hours for hours in range(24) if sm_df[(sm_df["T_bin"] == tmin) & (sm_df.index.hour == hours)][phase_col].dropna().empty]

                    if missing_hours:  # If any hour is missing, this bin is not valid
                        valid_bins_found = False
                        break  # stop checking phases, try fewer bins

                    tmin_list.append(tmin)

                if not valid_bins_found:
                    n_bins -= 1  # Retry with fewer bins

            except Exception:
                n_bins -= 1
                continue

        if not valid_bins_found:
            logging.warning(f"Could not find a fully populated lowest temperature bin; using {n_bins} bins as fallback.")
            sm_df["T_bin"] = pd.qcut(sm_df["T"], n_bins, labels=range(n_bins), duplicates="drop")
            tmin_list = [sm_df["T_bin"].value_counts().idxmax()] * 3  # If no valid bin found, use the most common bin as fallback
        

        # Create a base load profile, based on consumption at different hours of the day at the specific temperature bin
        load_base_list = []
        for phase in range(3):
            loadb = pd.Series(
                # Sample the base load for each hour and specific temperature bin
                [
                    sm_df[(sm_df['T_bin'] == tmin_list[phase]) & (sm_df.index.hour == hour)][sm_df.columns[phase]].sample().values[0]
                    if not sm_df[(sm_df['T_bin'] == tmin_list[phase]) & (sm_df.index.hour == hour)].empty
                    else np.nan
                    for hour in sm_df.index.hour
                ],
                index=sm_df.index,
                name="load_base_" + str(sm_df.columns[phase])
            ).interpolate(method='linear').bfill().ffill()
            load_base_list.append(loadb)

        return load_base_list, tmin_list, n_bins

    # Method to get the thermal priors
    def _get_priors(self, dfl, dfb, nsamples=1000, maxstep=4000, step=40, tmin = list(range(12)), nT=12):
        
        # Get thermal priors by sampling parameters of a truncated normal distribution
        mu_prior = np.zeros((24,nT))
        sd_prior = np.zeros((24,nT))
        
        for hour in range(24):
            for t_bin in range(nT):
                load = dfl[(dfl.index.hour==hour) & (dfb==t_bin)]
                loadb = dfl[(dfl.index.hour==hour) & (dfb==tmin)]

                threshold_index = int(nsamples / 100)
                counter = 0

                while True:
                    mu = np.random.uniform(0, maxstep, nsamples)
                    sd = np.random.uniform(0, maxstep/4, nsamples)
                    distance = np.zeros(nsamples)

                    total_load, _ = np.histogram(load, bins=step, range=(0, maxstep))
                    total_load = (total_load + 1.e-6) / (total_load + 1.e-6).sum() # avoid NaNs
                        
                    for i in range(nsamples):
                        samples_base = np.random.choice(loadb, 1000)
                        lim_a, lim_b = (0 - mu[i]) / sd[i], (maxstep - mu[i]) / sd[i]
                        samples_thermal = truncnorm.rvs(lim_a, lim_b, loc=mu[i], scale=sd[i], size=1000)
                        
                        # Approximate the total load, by sampling from the base load and thermal load
                        approx_load, _ = np.histogram(samples_base + samples_thermal, bins=step, range=(0, maxstep))
                        approx_load = (approx_load + 1.e-6) / (approx_load + 1.e-6).sum() # avoid NaNs

                        distance[i] = (total_load * (np.log(total_load) - np.log(approx_load))).sum()

                    # Select the mu and sd values that give the lowest distance
                    threshold = np.sort(distance)[threshold_index]
                    selected_mu = mu[distance < threshold]
                    selected_sd = sd[distance < threshold]
        
                    # Increase the threshold, if we cannot find any valid mu and sd values
                    if counter > 2:
                        threshold_index += 1
                        counter = 0
                        continue

                    if selected_mu.size == 0 or selected_sd.size == 0:
                        counter += 1
                        if threshold_index >= nsamples:
                            raise ValueError("Unable to find a valid threshold without NaNs.")
                        continue

                    if not np.isnan(selected_mu.mean()) and not np.isnan(selected_sd.mean()):
                        break

                    threshold_index += 1
                    if threshold_index >= nsamples:
                        raise ValueError("Unable to find a valid threshold without NaNs.")

                mu_prior[hour, t_bin] = selected_mu.mean()
                sd_prior[hour, t_bin] = selected_sd.mean()


        # From the mu and sd parameters, create a list of n_maxstep values for the truncated normal distribution for each hour and temperature bin
        thermal_prior = np.zeros((step, 24, nT))
        for hour in range(24):
            for t_bin in range(nT):
                for a, xa in enumerate(np.linspace(0,maxstep,step,endpoint=False)):
                    lim_a, lim_b = (0 - mu_prior[hour,t_bin]) / sd_prior[hour,t_bin], (maxstep - mu_prior[hour,t_bin]) / sd_prior[hour,t_bin]
                    thermal_prior[a,hour,t_bin] = truncnorm(lim_a, lim_b, loc=mu_prior[hour,t_bin], scale=sd_prior[hour,t_bin]).pdf(xa)

        # Normalize the thermal prior distributions
        thermal_prior += 1.0e-6
        thermal_prior /= thermal_prior.sum(axis=0)
        
        return thermal_prior

    # Method to get the posterior distribution
    def _get_posterior(self, prior, phase, maxstep, step, sm_df, tmin, nT=12):

        # Set up the likelihood, by counting the occurrences of the load at different hours of the day and temperature bins
        likelihood = np.zeros((step, step, 24, nT))
        for hour in range(24):
            hour_loadb, _ = np.histogram(sm_df[sm_df.columns[phase]][(sm_df.index.hour==hour) & (sm_df['T_bin']==tmin)], bins=step, range=(0, maxstep))
            for t_bin in range(nT):
                for a, xa in enumerate(np.linspace(0,maxstep,step,endpoint=False)):
                    for b, xb in enumerate(np.linspace(0,maxstep,step,endpoint=False)):
                        likelihood[b,a,hour,t_bin] = hour_loadb[np.clip(int((xb - xa) / 100), 0, step-1)]

        # Set up the evidence by marginalizing through all combinations of likelihood and prior
        evidence = np.zeros((step, 24, nT))
        for hour in range(24):
            for t_bin in range(nT):
                for b, xb in enumerate(np.linspace(0,maxstep,step,endpoint=False)):
                    evidence[b,hour,t_bin] = (likelihood[b,:,hour,t_bin] * prior[:,hour,t_bin]).sum()

        # Set up the posterior using Bayes' theorem
        posterior = np.zeros((step, step, 24, nT))
        for hour in range(24):
            for t_bin in range(nT):
                for a, xa in enumerate(np.linspace(0,maxstep,step,endpoint=False)):
                    for b, xb in enumerate(np.linspace(0,maxstep,step,endpoint=False)):
                        posterior[a,b,hour,t_bin] = likelihood[b,a,hour,t_bin] * prior[a,hour,t_bin] / evidence[b,hour,t_bin] if evidence[b,hour,t_bin] > 0 else 0

        # Normalize the posterior distributions
        posterior += 1.0e-6
        posterior /= posterior.sum(axis=0)

        return posterior
    
    # Method to get the thermal prior distributions
    def _get_thermal_prior(self, sm_df, tmin_list, n_bins):
        
        # Get the thermal prior distribution for each phase
        # Divide the load into steps of 100, up to the maximum load rounded up to the nearest 1000
        thermal_prior_list = []
        maxstep_list = [] # maximum load
        step_list = [] # step size
        
        for phase in range(3):

            # If the sum of one phase is less than 1% of the sum of the two other phases, then we regard it as dead
            sum_phase = sm_df[sm_df.columns[phase]].sum(axis=0)
            other_phases = [p for p in range(3) if p != phase]

            if sum_phase == 0 or all(sum_phase < 0.01 * sm_df[sm_df.columns[p]].sum(axis=0) for p in other_phases):
                maxstep_list.append(0)
                step_list.append(0)
                thermal_prior_list.append(np.zeros((100, 24, 12)))
                continue
            
            # Find the maxstep and step
            maxstep = int(np.ceil(sm_df[sm_df.columns[phase]].max()/1000)*1000)
            maxstep_list.append(maxstep)
            step = int(maxstep/100)
            step_list.append(step)                

            # Get a array of normalized thermal prior distributions for each hour of the day and temperature bin
            thermal_prior = self._get_priors(sm_df[sm_df.columns[phase]], sm_df['T_bin'], nsamples=int(100), maxstep = maxstep, step=step, tmin = tmin_list[phase], nT = n_bins)
            thermal_prior_list.append(thermal_prior) # Add them to the thermmal prior list, one for each phase
        
        return thermal_prior_list, maxstep_list, step_list
    
    # Method to get the thermal posterior distributions and sample thermal loads
    def _get_thermal_posterior(self, thermal_prior_list, maxstep_list, step_list, sm_df, tmin_list, n_bins):
        
        # Get the thermal posteriors
        thermal_posterior_list = []
        for phase in range(3):

            maxstep = maxstep_list[phase]
            step = step_list[phase]

            # If the phase is dead, then append a zero array
            if maxstep == 0:
                thermal_posterior_list.append(np.zeros((100, 24, n_bins)))
            else:
                thermal_prior = thermal_prior_list[phase]
                thermal_posterior = self._get_posterior(thermal_prior, phase, maxstep, step, sm_df, tmin_list[phase], n_bins)
                thermal_posterior_list.append(thermal_posterior)

        # Sample a posterior thermal load for each phase
        post_thermal_load = []
        for phase in range(3):
            step = step_list[phase]
            maxstep = maxstep_list[phase]

            if maxstep == 0:
                # Append a Series of zeros
                post_thermal_load.append(pd.Series(np.zeros(len(sm_df)), index=sm_df.index))
            else:
                thermal_posterior = thermal_posterior_list[phase]
                series = pd.Series(
                    [np.random.choice(100 * np.arange(0, step), p=thermal_posterior[:, load, hour, t_bin])
                        for load, hour, t_bin in zip((sm_df[sm_df.columns[phase]] / 100).astype(int).clip(0, step - 1), sm_df.index.hour, sm_df['T_bin'])],
                    index=sm_df.index
                )
                post_thermal_load.append(series)
            
        return post_thermal_load
    
    # Method to update the meter table with heat pump and solar panel information
    def _calculate_temperature_linearity(self, sm_df, post_thermal_load, maxstep_list):
        
        # Filter for low temperature days
        # This creates more robust results, as the temperature dependency is more visible at low temperatures
        sm_df['T_filtered'] = sm_df['T'].round().astype(int)
        valid_dates = sm_df[(sm_df['T_filtered'] <= 3)].index

        # Compute the slope of the post_thermal_load vs temperature
        # Use these results to determine if the sm has a heat pump / temperature dependent load
        slopelist = []
        interceptlist = []
        for phase in range(3):
            mask = post_thermal_load[phase].index.isin(valid_dates)

            # If the phase is dead, then append zeros
            if maxstep_list[phase] == 0:
                slopelist.append(0)
                interceptlist.append(0)
                continue
            
            subset_data = post_thermal_load[phase].loc[mask]
        
            if not subset_data.empty:

                X = sm_df.loc[mask, 'T_filtered'].values.reshape(-1, 1)
                y = subset_data.values
                
                model = LinearRegression()
                model.fit(X, y)
                slope = model.coef_[0]
                intercept = model.intercept_
                interceptatmin = intercept + slope * sm_df['T_filtered'].min()

                slopelist.append(slope)
                interceptlist.append(interceptatmin)
            else:
                slopelist.append(0)
                interceptlist.append(0)

        maxslopelist = []
        heat_pump = []
        hp_indicators = 0
        for phase in range(3):  # Compute if the phase is below the threshold of -2.7% per degree C
            maxslopelist.append(-0.027 * interceptlist[phase])
            if slopelist[phase] < maxslopelist[phase]:
                heat_pump.append(True)
                hp_indicators += 1
            else:
                heat_pump.append(False)

        # If there is two/three phases, we might have a problem with the threshold. Give some extra leeway
        if hp_indicators == 2: 
            for phase in range(3):
                    if slopelist[phase] > maxslopelist[phase]:
                            maxslopelist[phase] = maxslopelist[phase] * (1-0.25)
                            if slopelist[phase] < maxslopelist[phase]:
                                hp_indicators += 1
                                heat_pump[phase] = True

        return heat_pump, hp_indicators, slopelist, interceptlist, maxslopelist
    
    # Method to get the ANOVA results
    def _get_anova_results(self, sm_df, post_thermal_load):

        # Prepare the data for ANOVA analysis
        post_thermal_load = pd.DataFrame(post_thermal_load).T
        post_thermal_load['Hour'] = post_thermal_load.index.hour
        post_thermal_load['T_filtered'] = sm_df['T'].round().astype(int)
        anova_data = pd.melt(post_thermal_load.reset_index(), id_vars=['Hour', 'T_filtered'],
                            # Use the posterior thermal load columns for the value_vars
                            value_vars=[phase for phase in post_thermal_load.columns if phase not in ['Hour', 'T_filtered']],
                            var_name='Thermal_Load', value_name='Load')

        # Filter the data for the ANOVA analysis and run the ANOVA
        anova_data = anova_data[(anova_data['T_filtered'] <= self.thresholds['filter_temp'])] # Keep only from temperature < 3 deg
        
        # If there is only one unique value, ANOVA cannot be performed
        for col in ['T_filtered', 'Thermal_Load']:
            if anova_data[col].nunique() <= 1: 
                return None
        
        model = ols('Load ~ C(T_filtered) + C(Thermal_Load) + C(T_filtered):C(Thermal_Load)', data=anova_data).fit()
        anova_results = anova_lm(model, typ=2)
        return anova_results
    
    # Method to calculate the MAE and relative MAE
    def _calculate_mae(self, sm_df, post_thermal_load, loadb_list):
        # Compute the MAE and relative MAE between the post_thermal_load and the base load
        total_load = [post_thermal_load[phase] + loadb_list[phase] for phase in range(3)]
        mae_list = []
        maer_list = []
        for phase in range(3):
            mae_list.append(mean_absolute_error(total_load[phase], sm_df.iloc[:, phase]))
            maer_list.append(mean_absolute_error(total_load[phase], sm_df.iloc[:, phase]) / sm_df.iloc[:, phase].mean())
        
        return mae_list, maer_list
    
    # Method to filter the smart meter data based on weekly changes
    def _filter_data(self, sm_df):
        weekly_mean = sm_df.resample('W').mean()
        weeks_to_keep = weekly_mean[weekly_mean.pct_change().abs().mean(axis=1) > self.thresholds['weekly_change']].index
        sm_df = sm_df[sm_df.index.tz_localize(None).to_period('W').isin(weeks_to_keep.tz_localize(None).to_period('W'))]
        return sm_df

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

        # Find earlier results files in the results directory
        earlier_results_paths = [os.path.join(self.results_dir, f) for f in os.listdir(self.results_dir) if "results" in f]

        def _label_meters(sm_ids):

            # Load heat pump status and weather data

            sm_with_hp = self.topology_controller.get_meters(has_heat_pump=True)

            weather_df = self.data_extractor.s3_connector.read_small_csv(self.weather_file)

            # Set up meta results file
            meta_results_chunk = {sm_id: {"Heat_pump_info": [], "Heat_pump_stats": [], 
                                          "Summerhouse_info": "", "Summerhouse_stats": [],
                                          "ANOVA_info": "", "ANOVA_results": [],
                                          "MAE": [], "MAEr": [], "n_t_bins": ""} for sm_id in sm_ids}
            
            # Set up a dictionary to store the heat pump results. This will include all the new results
            heat_pump_results = {}

            # Set up a dictionary to store the heat pump returns. This will NOT include all the new results
            heat_pump_returns = {}

            for sm_id in tqdm(sm_ids, desc="HP Labeling on cleaned profiles of smart meters"):    

                logging.info(f"Processing smart meter {sm_id}")

                # If the sm_id does not have a heat pump, skip it
                if self.process_only_sm_with_hp and not sm_id in sm_with_hp:
                    logging.info(f"Smart meter {sm_id} does not have a heat pump. Skipped.")
                    continue
                
                # Check if results already exist in earlier results files
                if not self.overwrite_existing_results:
                    proceed_with_processing = True
                    heat_pump_returns, proceed_with_processing = self._check_previous_results(earlier_results_paths, sm_id, heat_pump_returns)
                    if proceed_with_processing is False:
                        logging.info(f"Skipping processing for smart meter {sm_id} as results already exist.")
                        continue

                # Load the cleaned sm data
                sm_df = self.data_extractor.v1_get_single_meter_data(sm_id)

                # Keep only the active power columns
                sm_df = sm_df[[col for col in sm_df.columns if 'active_power_p14' in col]]
                
                # But don't keep columns that have _status in it
                sm_df = sm_df[[col for col in sm_df.columns if '_status' not in col]]

                # Remove rows with "missing data imputed"
                sm_df = sm_df[~sm_df.isin(['missing data imputed']).any(axis=1)]
                sm_df = sm_df.fillna(0)

                sm_df.columns = [f"l{phase+1}" for phase in range(sm_df.columns.size)]

                sm_df.index = pd.to_datetime(sm_df.index).tz_convert('UTC')

                if self.label_summerhouse:
                    meta_results_chunk = self._label_summerhouse(sm_df, sm_id, meta_results_chunk)
                
                if self.filter_data:
                    sm_df = self._filter_data(sm_df)

                # Plotting the filtered data
                if self.save_plots:
                    self._plot_sm(sm_id, sm_df, self.results_dir, filename="filtered")

                # Resample to hourly data
                sm_df = sm_df.resample('1h').mean(numeric_only=True)

                # Join weather data
                sm_df = sm_df.join(self.weather_df, how='left')

                # Remove the periods where all the phases are zero
                sm_df = sm_df[(sm_df[sm_df.columns[0]] != 0) | (sm_df[sm_df.columns[1]] != 0) | (sm_df[sm_df.columns[2]] != 0)]
                
                # Get the base load and the lowest consumpting temperature bin for each phase
                loadb_list, tmin_list, n_bins = self._get_base_load_and_temperature_bins(sm_df)

                sm_df["T_bin"] = pd.qcut(sm_df["T"], n_bins, labels=range(n_bins))

                # Find the thermal priors for each phase
                thermal_prior_list, maxstep_list, step_list = self._get_thermal_prior(sm_df, tmin_list, n_bins)

                # Find the thermal posterior for each phase, and sample a thermal load
                post_thermal_load = self._get_thermal_posterior(thermal_prior_list, maxstep_list, step_list, sm_df, tmin_list, n_bins)
                
                # Calculate the slope of the thermal load vs temperature, to determine if there is a heat pump
                # Save the slope and intercept values in the meta results
                heat_pump, hp_indicators, slopelist, interceptlist, maxslopelist = self._calculate_temperature_linearity(sm_df, post_thermal_load, maxstep_list)

                if save_meta_results:
                    # Save number of temperature bins used
                    meta_results_chunk[sm_id]["n_t_bins"] = str(n_bins)

                    # Save heat pump info
                    meta_results_chunk[sm_id]["Heat_pump_info"].extend(f'{sm_df.columns[phase]}: {heat_pump[phase]}' for phase in range(3))
                    meta_results_chunk[sm_id]["Heat_pump_stats"].extend(f'slope {sm_df.columns[phase]}: {slopelist[phase]:.4f} (Threshold: {maxslopelist[phase]:.4f})' for phase in range(3))
                    meta_results_chunk[sm_id]["Heat_pump_stats"].extend(f'Intercept {sm_df.columns[phase]}: {interceptlist[phase]:.2f} Wh' for phase in range(3))
                
                    # Calculate MAE and MAEr
                    mae_list, maer_list = self._calculate_mae(sm_df, post_thermal_load, loadb_list)
                    meta_results_chunk[sm_id]["MAE"].extend(f"{sm_df.columns[phase]}: {mae_list[phase]:.4f}" for phase in range(3))
                    meta_results_chunk[sm_id]["MAEr"].extend(f"{sm_df.columns[phase]}: {maer_list[phase]:.4%}" for phase in range(3))
                
                if self.use_ANOVA:
                    anova_results = self._get_anova_results(sm_df, post_thermal_load)

                    if save_meta_results:

                        if anova_results is None or (isinstance(anova_results, pd.DataFrame) and anova_results.empty):
                            meta_results_chunk[sm_id]["ANOVA_info"] = f"ANOVA could not be computed due to insufficient data variation"
                        
                        if anova_results is not None:
                            # Save meta results on ANOVA
                            if anova_results['PR(>F)']['C(Thermal_Load)'] > self.thresholds['anova_pvalue']:
                                meta_results_chunk[sm_id]["ANOVA_info"] = f"We cannot discard the idea of a relation between the thermal loads"
                                
                                if hp_indicators < 2:
                                    meta_results_chunk[sm_id]["Heat_pump_info"] = ["Does likely not have a heat pump, based on slope and ANOVA"]
                                    meta_results_chunk[sm_id]["Heat_pump_info"].extend(f'{sm_df.columns[phase]}: False' for phase in range(3))
                                    heat_pump = [False, False, False]
                                elif hp_indicators >= 2:
                                    meta_results_chunk[sm_id]["Heat_pump_info"] = ["Does likely have a heat pump, based on slope and ANOVA"]
                                    meta_results_chunk[sm_id]["Heat_pump_info"].extend(f'{sm_df.columns[phase]}: True' for phase in range(3))
                                    heat_pump = [True, True, True]
                            else:
                                meta_results_chunk[sm_id]["ANOVA_info"] = f"There is no relation between the thermal loads"

                            meta_results_chunk[sm_id]["ANOVA_results"].append(f'P-value (Thermal_Load): {anova_results["PR(>F)"]["C(Thermal_Load)"]:.4%}')
                            meta_results_chunk[sm_id]["ANOVA_results"].append(f'P-value ((T_filtered):C(Thermal_Load)): {anova_results["PR(>F)"]["C(T_filtered):C(Thermal_Load)"]:.4%}')

                    else:
                        if anova_results is not None:
                            if anova_results['PR(>F)']['C(Thermal_Load)'] > self.thresholds['anova_pvalue']:
                                if hp_indicators < 2:
                                    heat_pump = [False, False, False]
                                elif hp_indicators >= 2:
                                    heat_pump = [True, True, True]
                    
                # Return dictonary with heat pump results
                heat_pump_results[sm_id] = {"l1": bool(heat_pump[0]),
                                             "l2": bool(heat_pump[1]),
                                             "l3": bool(heat_pump[2])}
                
                # Add to the heat pump return
                heat_pump_returns[sm_id] = heat_pump_results[sm_id]

            return meta_results_chunk, heat_pump_results, heat_pump_returns

        if self.use_dask:
            sm_id_chunks = np.array_split(self.sm_ids, min(len(self.sm_ids), self.n_workers))
            delayed_tasks = [delayed(_label_meters)(sm_ids=sm_ids_chunk) for sm_ids_chunk in sm_id_chunks]
            # meta_results_list = compute(*delayed_tasks)

            results = compute(*delayed_tasks)
            meta_results_list = [r[0] for r in results]
            heat_pump_list = [r[1] for r in results]
            heat_pump_returns = [r[2] for r in results]

            meta_results_merged = {k: v for d in meta_results_list for k, v in d.items()}
            heat_pump_merged = {k: v for d in heat_pump_list for k, v in d.items()}
            heat_pump_returns = {k: v for d in heat_pump_returns for k, v in d.items()}
            sleep(1)
        else:
            meta_results_merged, heat_pump_merged, heat_pump_returns = _label_meters(sm_ids=self.sm_ids)
        
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
            task_id="Label phases of SMs",
            python_callable=statlabeler.run,
            doc_md="""
            ## Label phases of SMs
            """,
        )

        stat_labeler_task

    
