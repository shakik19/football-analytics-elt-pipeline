FROM quay.io/astronomer/astro-runtime:11.3.0

WORKDIR /usr/local/airflow/

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir astronomer-cosmos[dbt-bigquery] && deactivate