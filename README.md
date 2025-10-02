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
1) You need to have Docker installed and running as a desktop application (can be downloaded from https://www.docker.com/)
2) You need to have sqitch (a database migration tool) installed. You can install it from the terminal using homebrew (macOS only):
    '''
    brew tap sqitchers/sqitch
    brew install sqitch --with-postgres-support
    '''
    If you're on Windows, there are other ways to get sqitch.
3) You also need to have psql installed (a command-line cliet for PostgreSQL databases). This will be picked up during the build process to install the custom 3phi-framework python library. You can install it from the terminal using homebrew (macOS only). The second command ads pqsl to the path:
    '''
    brew install libpq
    echo 'export PATH="/usr/local/opt/libpq/bin:$PATH"' >> ~/.zshrc
    '''
    After adding to the path you need to restart the shell. If you're on Windows, there are other ways to get psql.
4) Create a file in the top folder of the repository (the folder is called "data-platform"). The new file should be named "pip.conf". Copy the following into the file:
    ```
    [global]
    index-url = https://<GL_USER>:<GL_TOKEN>@gitlab.3pi-dev.io/api/v4/projects/4/packages/pypi/simple
    extra-index-url = https://pypi.org/simple
    ```
   Replace `<GL_USER>` with your Gitlab username and replace `<GL_TOKEN>` with a gitlab access token provided by Chris (the token is basically a string of characters, like a password).
5) In your terminal, navigate to the "data-platform" folder and execute the following command:
    ```
    make up HOST=localhost PORT=5432 DB_USER=postgres PASSWORD=password ROLE=threephi_db_user ROLE_PW=userpass DB_NAME=3phi-db  
    ```
    The Docker container with database, dask cluster and airflow will be created locally on your machine. A user will be created with the name you provide as ROLE, and password you provide as ROLE_PW. Working locally this is rather arbitrary (merely 'pro forma') and you will not need to remember those. At last, the database will be deployed locally on your machine.

## Custom Docker Images

### Airflow
The custom airflow image is built so the defined dags and the source code are available to the Airflow containers.

### Dask
The custom dask image is built, so the source code is bundled directly in the image.

## TODOs
- S3 Initialization: Create Buckets and Users automatically
- "One command" local platform
