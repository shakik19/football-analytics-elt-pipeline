![Python 3](https://img.shields.io/badge/Python-3.11.2-green?style=flat&logo=python&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google_Cloud-3772FF?style=flat&logo=googlecloud&logoColor=white&labelColor=3772FF)
![dbt Version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.8.3&color=orange)
![Terraform](https://img.shields.io/badge/Terraform-1.8.5-black?style=flat&logo=terraform&logoColor=white&labelColor=573EDA)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.9.2-black?style=flat&logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-blue?style=flat&logo=docker&logoColor=white)

# Transfermarkt Data Pipeline
This project is an end-to-end ELT batch data pipeline, built with a modern data stack and optimized for efficient data processing and analysis of club football using [transfermarkt](https://www.transfermarkt.com) data. It is fully configured for easy deployment on [Astronomer Cloud](https://www.astronomer.io) streamlining the entire process.


## Table of Contents
- [Transfermarkt Data Pipeline](#transfermarkt-data-pipeline)
  - [Table of Contents](#table-of-contents)
  - [Objectives](#objectives)
  - [Running the Pipeline](#running-the-pipeline)
  - [Pipeline Architecture](#pipeline-architecture)
  - [Data Stack](#data-stack)
  - [Workflow Description](#workflow-description)
    - [Infrastructure Setup](#infrastructure-setup)
    - [Extract](#extract)
    - [Pre-Process](#pre-process)
    - [Load](#load)
    - [Transform](#transform)
    - [CI/CD Pipeline](#cicd-pipeline)
    - [Alerting, Logging, and Monitoring](#alerting-logging-and-monitoring)
  - [Data Visualization](#data-visualization)
  - [Improvements](#improvements)

## Objectives

This project aims to enable sports analysts to derive key insights about the football clubs, matches,players and the transfer market. A dashboard has been created about football club's seasonal stats using the final dataset to showcases the application

## Running the Pipeline

For detailed instructions on how to run the pipeline, please refer to the following markdown files:

- [Running Locally](./docs/running_locally.md)
- [Running in Production](./docs/running_in_astro.md)

## Pipeline Architecture
<div style="text-align: center;">
  <img src="./assets/architecture/architecture.jpg" alt="Architecture">
</div>

The architecture of this project is designed to efficiently handle data extraction, processing, and transformation while ensuring scalability and reliability. The core components are integrated to streamline the data flow from raw sources to final analytical outputs, following industry best practices and leveraging modern data engineering tools.

## Data Stack

- **Orchestration:** Astronomer managed Apache Airflow
- **Data Lake:** Google Cloud Storage
- **Data Warehouse:** Google BigQuery
- **Data Transformation:** DBT
- **Infrastructure as Code:** Terraform
- **Containerization:** Docker
- **Others:** Pyarrow and Pandas

## Workflow Description
Airflow directed acyclic graph
<div style="text-align: center;">
  <img src="./assets/airflow/dag.jpg" alt="DAG">
</div>

### Infrastructure Setup

Terraform is used to provision all the cloud resources. Astronomer deployment, Google Cloud Storage and Google BigQuery. Refer to the [infrastructure directory](./infrastructure) for details.

### Extract

Raw data is pulled from Kaggle via API and stored locally using a reusable Python module. For larger datasets, direct streaming to Google Cloud Storage (GCS) is more suitable, though it comes with trade-offs like increased latency and complexity but has benifits such as availability, durability and almost infinite storage.

**Security Note:** The only method to authenticate with Kaggle api without having a physical json file is through environment variables as it isn't meant for using in such data pipelines.

### Pre-Process

A corrupt file with inconsistent columns are handled. Pyarrow's auto schema detection is used to enforce a primary schema and then files are serialized to Parquet format for compression and schema retention. The whole process in done using in parallel using multithreading.

### Load

Parquet files are loaded into Google Cloud Storage and then into Google BigQuery as staging (Silver) data.

### Transform

<div style="text-align: center;">
  <img src="./assets/visualization/dbt_lineage_graph.png" alt="dbt-lineage-graph">
  <p>DBT Lineage Graph</p>
</div>

Using DBT, data from the staging dataset is transformed into Gold level data. This process includes adding new values through extraction, filtering unnecessary values, fixing inconsistencies, reinforcing schemas if required, and some data modeling.

The data model of the warehouse uses the **Snowflake schema**. A snowflake schema is a type of dimensional modeling that builds upon the star schema concept. It offers a more normalized structure, suitable for data warehouses with complex data models and intricate relationships between dimensions.

<div style="text-align: center;">
  <img src="./assets/data model/partial_data_model.jpg" alt="data-model">
  <p>Partial data model</p>
</div>

**Why snowflake schema?**
As you can see in the ER diagram, the fact tables rely on common dimensional tables as a result normalization minimizes duplication of data across dimension tables, leading to more efficient storage usage. Normalization also helps to prevent data inconsistencies. I also want to mention that from source the data came already came in a very normalized form. I only merged two fact tables to one fact and dim tables for my flexibility and added a country code dimension for analysis that requires geographical data. 

**Best practices and optimizations:**

1. **Validating Tests:** Ensuring data accuracy through validation checks.
2. **Incremental Materialization:** Processing only new data to improve efficiency and reduce computational costs.
3. **Partitioning and Clustering:** Organizing data based on hypothetical needs to speed up query performance and retrieval times.

**Issue:** Initially, materialized views were used for reporting tables to enhance data retrieval by pre-computing and storing data at refresh intervals. However, due to limitations with BigQuery's materialized views, regular views are now used, which only store the query logic.

### CI/CD Pipeline

For production setup, CI/CD is enforced using GitHub Actions to Astro deployment. Refer to the [Running the Pipeline in Production](./docs/running_in_astro.md) section for more details.

### Alerting, Logging, and Monitoring

This section hasn't been configured yet but I have a plan to make a logging data pipeline using the ELK stack in the near future.

## Data Visualization
The interactive dashboard below provides a comprehensive seasonal analysis of various football clubs, offering valuable insights into their performance and trends throughout the season.
<div style="text-align: center;">
  <img src="./assets/visualization/Celtic_2023.png" alt="Architecture">
</div>

*Due to security concerns I can't provide access to the interactive dashboard. The dashboard isn't totally static, on paremeter changes or after certain interval it queries data from BigQuery which isn't entirely free. You can find images of some other club's dashboard grouped by league [here](./assets/visualization/)*

## Improvements

1. Add more validation and freshness tests for DBT models.
2. Add tests for the DAGs.
3. Make a dashboard that groups all club data under one plane for better comparative insights.
4. Implement the logging pipeline
