import os
from datetime import datetime
from include.extractor import DatasetDownloader
from include.transformer import DataTransformer
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
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
    max_active_tasks=3
):
    PROJECT_NAME = "transfermarkt"
    GCS_BUCKET_NAME = os.getenv("BUCKET_NAME")
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

    bash_move_seed_command = f"mv {DATASET_DIR}/csv/competitions.csv {os.getenv('DBT_PROJECT_DIR')}/seeds"
    move_seed_files = BashOperator(
        task_id="move_seed_files",
        bash_command=bash_move_seed_command
    )

    """
    [Pre-transformation]
    Excluding some corrupt columns from the game_lineups.csv file
    The schema is inconsistent in the "number" and "team_captain columns
    """
    t_transformer = DataTransformer(project_name=PROJECT_NAME)
    with TaskGroup(group_id="pre_process_data") as pre_process_data:
        pt_columns = ["number", "team_captain"]
        pt_filename = "game_lineups"
        exclude_columns_task = PythonOperator(
            task_id="exclude_columns_task",
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

        exclude_columns_task >> convert_to_parquet


    """ 
    [Load]
    Upload files to GCS and load them into BQ
    """ 
    TABLES = t_transformer.TABLES
    with TaskGroup(group_id="load_raw_data") as load_raw_data:
        for table in TABLES:
            filename = f"{table}.parquet"
            src_file_path = os.path.join(t_transformer.PARQUET_DATASET_DIR, filename)
            gcs_file_path = os.path.join(PROJECT_NAME, filename)
            table_name = f"{os.getenv('PROJECT_ID')}.{os.getenv('SEED_DATASET_NAME')}.{table}"
            gcs_file_uri_prefix = os.path.join(GCS_BUCKET_NAME, gcs_file_path)
            
            """ 
            Uploading parquet files to Data Lake(Google Cloud Stroage) 
            """
            upload_to_gcs = LocalFilesystemToGCSOperator(
                task_id=f"upload_{table}",
                src=src_file_path,
                dst=gcs_file_path,
                bucket=GCS_BUCKET_NAME,
                gcp_conn_id=GCP_CONN_ID
            )
            
            query = f"""
                LOAD DATA OVERWRITE `{table_name}`
                FROM FILES (
                format = 'PARQUET',
                uris = ['gs://{gcs_file_uri_prefix}']
                );
            """

            """
            Loading data from Data Lake to Data Warehouse as native tables for 
            transformation and analysis
            """
            load_dataset_table = BigQueryInsertJobOperator(
                task_id=f'load_{table}_into_bigquery',
                gcp_conn_id=GCP_CONN_ID,
                configuration={
                    "query": {
                        "query": query,
                        "useLegacySql": False,
                    }
                },
                location=os.getenv("DATASET_LOCATION"),
                result_timeout=60
            )

            upload_to_gcs >> load_dataset_table

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

    start >> extract_raw_data >> move_seed_files >> pre_process_data >> load_raw_data 
    load_raw_data >> dbt_run_models >> clean_local_dataset >> end