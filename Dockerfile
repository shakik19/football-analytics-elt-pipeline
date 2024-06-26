FROM quay.io/astronomer/astro-runtime:11.5.0

WORKDIR /usr/local/airflow/

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir astronomer-cosmos && deactivate

ENV GCP_CONN_ID="google_cloud_default"
ENV DATASET_DIR="/usr/local/airflow/dataset"

# DBT VARIABLES
ENV DBT_PROJECT_DIR="/usr/local/airflow/dbt"
ENV DBT_EXE_PATH="/usr/local/airflow/dbt_venv/bin/dbt"