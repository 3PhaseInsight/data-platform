# 3PHI Data Platform

## Getting started

### Local Volumes

Run "make init-local", which will create some directories that are mounted in the docker containers.

### Data Directory

Place your meter_data files and hourly_measurement files in the ./data directory, 
so the containers can read data from there for the initial ingestion.

### Spinning up the platform

Just run:
```
docker compose up
```
This will create all necessary components and initialize the DB and the S3 Bucket.

The only manual step before running the preprocessor pipeline is to upload the meter_cabinet_connection.csv to `3phi/Data/sourcedata/second_batch`.

## Custom Docker Images

### Airflow
The custom airflow image is built so the defined dags and the source code are available to the Airflow containers.

### Dask
The custom dask image is built, so the source code is bundled directly in the image.

### s3init
This is just a Python runtime image which runs the init_s3.py script to create the S3 Bucket and some starting directories.

### timescaledb
This image includes the init.sql script to properly initialize the database.

## Architecture

The platform consists of the following components:
- TimescaleDB: Store the source dataset to retain querying options for potential future API
- Docker Image Registry: Store custom built docker images
- Minio/S3 Bucket: File Storage for all non-time series source data and all processed data
- Airflow cluster:
  - Airflow webserver: WebUI to manage and start DAGs
  - Airflow scheduler: Coordinates DAG executions
  - Airflow worker: Executer Node for DAGs
  - RedisDB: Acts as a Message Queue Broker for scheduling worker tasks
- Dask cluster:
  - Dask scheduler: Coordinates Dask tasks
  - Dask worker (x2): Workers to execute dask tasks (Tasks that use Dask Dataframes in the Python Code)

## DAGs

A DAG (or Directed Acyclic Graphs) is a model that encapsulates everything needed to execute a workflow.
In this data platform context a DAG can be seen as a data pipeline than runs through a series of predefined steps.

A DAG can be a single task or can chain multiple tasks after one another. 

See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html for additional documentation.

## Dependencies

All except the 3phi-framework package can be installed regularly.
To install 3phi-framework when building images that depend on this locally, 
you need to create a pip.conf(pip.conf) file in the root dir containing:
```
[global]
index-url = https://<GL_USER>:<GL_TOKEN>@gitlab.3pi-dev.io/api/v4/projects/4/packages/pypi/simple
extra-index-url = https://pypi.org/simple
```

## Potential next additions

- API Layer to query data directly from the TimescaleDB
- Resource tuning to make platform less resource hungry (this will also lead to a degradation in performance, but that might be acceptable)
