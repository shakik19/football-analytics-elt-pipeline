from airflow import DAG
from airflow.operators.python import ExternalPythonOperator, PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from datetime import datetime
from pipeline.extract_and_load import (download_dataset, 
                                       upload_to_gcs,
                                       create_bq_seed_dataset,
                                       clean_csvs,)


PROJECT_PATH = "/usr/local/airflow"
PYTHON_EXECUTABLE_PATH = f"{PROJECT_PATH}/el_venv/bin/python" 

DBT_PROJECT_PATH = f"{PROJECT_PATH}/dags/dbt"
DBT_EXECUTABLE_PATH = f"{PROJECT_PATH}/dbt_venv/bin/dbt"

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


with DAG(
    dag_id="main_dag",
    start_date=datetime(2020,1,1),
    schedule=None,
    catchup=False,
    tags=['pipeline']
) as dag:    

    extract_data = PythonOperator(
        task_id = "extract_from_api",
        # python = PYTHON_EXECUTABLE_PATH,
        python_callable = download_dataset,
    )

    upload_to_gcs = ExternalPythonOperator(
        task_id = "upload_to_datalake",
        python = PYTHON_EXECUTABLE_PATH,
        python_callable = upload_to_gcs,
    )

    load_data = ExternalPythonOperator(
        task_id = "load_in_data_warehouse",
        python = PYTHON_EXECUTABLE_PATH,
        python_callable = create_bq_seed_dataset,
    )

    # transform_data_with_dbt = ExternalPythonOperator()

extract_data >> upload_to_gcs >> load_data