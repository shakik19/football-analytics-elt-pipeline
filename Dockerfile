FROM quay.io/astronomer/astro-runtime:11.3.0

WORKDIR /usr/local/airflow/

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir astronomer-cosmos[dbt-bigquery] && deactivate

ENV DATASET_DIR="/usr/local/airflow/dataset"

# DBT VARIABLES
ENV DBT_PROJECT_DIR="/usr/local/airflow/dbt"
ENV DBT_EXE_PATH="/usr/local/airflow/dbt_venv/bin/dbt"
ENV DBT_PROFILES_NAME="default"
ENV DBT_PROFILES_TARGET="prod"