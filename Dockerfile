FROM quay.io/astronomer/astro-runtime:11.5.0

WORKDIR $AIRFLOW_HOME

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir astronomer-cosmos[dbt-bigquery] && deactivate

ENV GCP_CONN_ID="google_cloud_default"
ENV DATASET_DIR="$AIRFLOW_HOME/dataset"

# DBT VARIABLES
ENV DBT_PROJECT_DIR="$AIRFLOW_HOME/dbt"
ENV DBT_EXE_PATH="$AIRFLOW_HOME/dbt_venv/bin/dbt"