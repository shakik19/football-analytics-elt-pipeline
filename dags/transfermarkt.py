import os
from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from include.extractor import DatasetDownloader
from include.transformer import DataTransformer
from cosmos import (DbtTaskGroup,
                    ProjectConfig,
                    ProfileConfig,
                    ExecutionConfig,
                    RenderConfig)
from cosmos.constants import TestBehavior


with DAG(
    dag_id="transfermarkt",
    description="Transfermarkt ELT data pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    tags=["ELT", "gcp", "bigquery", "cloud_storage","dbt"],
    catchup=False,
    max_active_runs=1,
    max_active_tasks=5
):
    PROJECT_NAME = "transfermarkt"
    GCP_CONN_ID = os.getenv("GCP_CONN_ID")
    DATASET_DIR = os.getenv("DATASET_DIR")
    start = EmptyOperator(task_id="start")

    """
    [Extraction] 
    Get data from kaggle api in csv format 
    """
    dataset_name = "davidcariboo/player-scores"
    t_extractor = DatasetDownloader(project_name=PROJECT_NAME)
    extract_raw_data = PythonOperator(
        task_id="extract_raw_data",
        python_callable=t_extractor.download_dataset,
        op_args=[dataset_name]
    )


    """
    [Pre-transformation]
    Excluding some corrupt columns from the game_lineups.csv file
    The schema is inconsistent in the "number" and "team_captain columns
    """
    pt_columns = ["number", "team_captain"]
    pt_filename = "game_lineups"
    t_transformer = DataTransformer(project_name=PROJECT_NAME)
    pre_process_data = PythonOperator(
        task_id="pre_process_data",
        python_callable=t_transformer.exclude_corrupt_columns,
        op_args=[pt_filename, pt_columns]
    )

    """
    Converting csv files to parquet for compression and auto schema generation
    """
    convert_to_parquet = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=t_transformer.csv_to_parquet
    )


    """ 
    [Load]
    Uploading parquet files to Data Lake(Google Cloud Stroage) 
    """
    TABLES = t_transformer.TABLES
    with TaskGroup(group_id="upload_files_to_gcs", 
                   tooltip="Upload files to GCS") as upload_files_to_gcs:
        for table in TABLES:
            filename = f"{table}.parquet"
            src_file_path = os.path.join(t_transformer.PARQUET_DATASET_DIR, filename)
            dst_file_path = os.path.join(PROJECT_NAME, filename)
            task_id = f"upload_{table}"
            LocalFilesystemToGCSOperator(
                task_id=task_id,
                src=src_file_path,
                dst=dst_file_path,
                bucket=os.getenv("BUCKET_NAME"),
                gcp_conn_id=GCP_CONN_ID
            )

    """
    Loading data from Data Lake to Data Warehouse as native tables for 
    transformation and analysis
    """
    with TaskGroup(group_id="load_files_into_bigquery", 
                   tooltip="Load files into BigQuery") as load_files_into_bigquery:
        for table in TABLES:
            filename = f"{table}.parquet"
            filepath = os.path.join(PROJECT_NAME, filename)
            table_name = f"{os.getenv('PROJECT_ID')}.{os.getenv('SEED_DATASET_NAME')}.{table}"
            gcs_filepath = os.path.join(os.getenv("BUCKET_NAME"), filepath)
            query = f"""
                LOAD DATA OVERWRITE `{table_name}`
                FROM FILES (
                format = 'PARQUET',
                uris = ['gs://{gcs_filepath}']
                );
            """

            load_table = BigQueryInsertJobOperator(
                task_id=f'load_{table}_into_bigquery',
                gcp_conn_id=GCP_CONN_ID,
                configuration={
                    "query": {
                        "query": query,
                        "useLegacySql": False,
                    }
                },
                location=os.getenv("DATASET_LOCATION"),
                result_timeout=30
            )


    """
    [Transform]
    DBT transformation tasks
    """
    DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
    DBT_EXE_PATH = os.getenv("DBT_EXE_PATH")

    PROJECT_CONFIG = ProjectConfig(
        dbt_project_path=DBT_PROJECT_DIR
    )

    PROFILE_CONFIG = ProfileConfig(
        profile_name=os.getenv("DBT_PROFILES_NAME"),
        target_name=os.getenv("DBT_PROFILES_TARGET"),
        profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml"
    )

    EXECUTION_CONFIG = ExecutionConfig(
        dbt_executable_path=DBT_EXE_PATH,
    )

    RENDER_CONFIG = RenderConfig(
        emit_datasets=False,
        test_behavior=TestBehavior.AFTER_EACH,
        dbt_deps=True
    )

    dbt_run_models = DbtTaskGroup(
        group_id="dbt_run_models",
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=None
    )

    """
    END
    Before ending clering the local csv and parquet files
    """
    bash_remove_command = f"rm -rfv {t_transformer.PROJECT_DATASET_DIR}"
    clean_local_dataset = BashOperator(
        task_id = "clean_local_dataset",
        bash_command=bash_remove_command
    )

    end = EmptyOperator(task_id="end")


    start >> extract_raw_data >> pre_process_data >> convert_to_parquet
    convert_to_parquet >> upload_files_to_gcs >> load_files_into_bigquery
    load_files_into_bigquery >> dbt_run_models >> clean_local_dataset >> end