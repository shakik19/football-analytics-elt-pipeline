FROM quay.io/astronomer/astro-runtime:10.6.0

# install dbt into a venv to avoid package dependency conflicts
WORKDIR "/usr/local/airflow"
COPY dbt-requirements.txt ./
RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate

# install data extractor and loader dependencies into a venv to avoid package dependency conflicts
WORKDIR "/usr/local/airflow"
RUN python -m virtualenv el_venv && source el_venv/bin/activate && \
    pip install --no-cache-dir -r el-requirements.txt && deactivate