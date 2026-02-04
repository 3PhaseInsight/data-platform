# 3-Phase-Insight Data Platform

## Introduction
This repo contains all necessary parts to spin up the 3-Phase-Insight (3Phi) Data Platform. It is structured in a way that 
divides the individual components of the platform into three different parts:

- data-platform-infrastructure: contains everything that's related to the storage infrastructure, such as the Database and the MinIO Bucket Storage
- data-platform-frontend: contains the user facing part of the platform, namely the Apache Airflow Webserver
- "the rest": contains the backend part of the platform, which consists of the Apache Airflow Scheduler & Worker as well as the Dask Cluster

### Apache Airflow
Airflow is used as a task orchestration framework in this platform. It includes a Python Environment which allows running Python Code.
The concept of DAGs allows defining workflows, which may also be called "Pipelines" in the 3Phi context. Airflow uses Redis as a 
Message Queue Broker for scheduling worker tasks.

A DAG (or Directed Acyclic Graphs) is a model that encapsulates everything needed to execute a workflow.
In this data platform context a DAG can be seen as a data pipeline than runs through a series of predefined steps.

A DAG can be a single task or can chain multiple tasks after one another.

See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html for additional documentation.

### Dask
Dask is a Python library for parallel and distributed computing. Pythons single-threaded nature limits compute- and processing-heavy
workflows. Dask solves this issue by introducing a Cluster consisting of the Dask Scheduler and one or more Dask Workers.

In simple words, the way Dask works, is that when Python statements using the dask library are executed, an "Execution Graph" is created,
without actually triggering any computation at first. The Execution Graph is basically just instruction after instruction and describes what
is intended to be done. Certain dask expressions, such as `.compute()` trigger the actual computation. When this happens, the serialized Execution Graph is passed from the Scheduler to the Workers, which then begin executing the steps that the Graph contains. 
The Dask Library takes care of how to split workloads up over multiple workers and join the results back together after the execution.

### MinIO
The platform (by default) uses MinIO as its object storage solution. MinIO is S3-compatible and optimized for large-scale data pipelines that require
a high performance storage solution.

### Data Directory
Place your meter_data files and hourly_measurement files in the ./data directory, 
so the containers can read data from there for the initial ingestion.

## Local Development

### Getting started
#### 1. Docker Installation
You need to have `Docker` installed and running as a desktop application. It can be downloaded from `https://www.docker.com`. You don't need to login as a user.

#### 2. Packet Manager Installation
You need a packet manager installed to be able to install further prerequisites from your terminal. 

**macOS users:**
If you don't have `Homebrew` installed, open your terminal, paste in the following command at the prompt and press 'Enter':
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

**Windows users:**
If you don't have `Chocolatey` installed:
1. Open PowerShell as Administrator (right-click Start button → "Windows PowerShell (Admin)")
2. Paste in the following command at the prompt and press 'Enter':
```powershell
iwr https://chocolatey.org/install.ps1 -UseBasicParsing | iex
```

#### 3. Creating a pip.conf file
Create a file in the root of the repository (the folder is called `data-platform`). The new file should be named `pip.conf`. Copy the following into the file:
```
[global]
index-url = https://<GL_USER>:<GL_TOKEN>@gitlab.3pi-dev.io/api/v4/projects/4/packages/pypi/simple
extra-index-url = https://pypi.org/simple
```
Replace `<GL_USER>` with your Gitlab username and replace `<GL_TOKEN>` with a Gitlab access token provided by an Admin (Token might need "Owner" permissions due to Gitlab quirks). The pip.conf file will be picked up during the build process to install the custom 3phi-framework python library.

#### 4. Prepare data directory
If not specifying a data directoy, the default behaviour requires a directory `data` to be created at the root of the repository. 

#### 5. Deploy the platform locally and initialize the Database
If you are working in MacOS you should already have `make` installed, but if you work on a Windows machine you need to install it:

```powershell
choco install make
```

Now, in your terminal, navigate to the "data-platform" folder and execute the following command:
```
make up
```
This will create the Docker containers (database, dask cluster and airflow) locally on your machine. A user will be created named threephi_db_user. This user will be used by the other services when they interact with the database. At last, the database will be initialized with the necessary schemas & tables.

#### 6. Create MinIO buckets
Open the MinIO Console at http://localhost:19001 (or in Docker Desktop, click the minio container’s Console port). Sign in with `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` from `.env` (defaults: `minioadmin` / `minioadmin`). Create these buckets:
- `3phi` — ingested timeseries parquet files
- `airflow-logs` — Airflow task logs

#### 7. Access the Airflow UI
Open http://localhost:8080 (or click `8080:8080` in Docker Desktop for the Airflow webserver). Retrieve the admin password from the webserver logs at Docker Desktop → Airflow-webserver → Logs. Look for:
```
Simple auth manager | Password for user 'admin': <16-character-password>
```
First run: reserialize DAGs so they appear by running `airflow dags reserialize` at Docker Desktop → Airflow-webserver → Exec.

## Custom Docker Images

### Airflow
The custom airflow image is built so the defined dags and the source code are available to the Airflow containers.

### Dask
The custom dask image is built, so the source code is bundled directly in the image.
