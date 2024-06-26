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
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import TestBehavior

ENV_NAME = os.getenv("ENV_NAME")
if ENV_NAME == "dev":
    SCHEDULE_INTERVAL = None
    TM_DAG_MAX_ACTIVE_RUNS = int(os.getenv("TM_DAG_MAX_ACTIVE_RUNS"))
    TM_DAG_MAX_ACTIVE_TASKS = int(os.getenv("TM_DAG_MAX_ACTIVE_TASKS"))
elif ENV_NAME == "prod":
    SCHEDULE_INTERVAL = "15 6 * * 3"
    TM_DAG_MAX_ACTIVE_RUNS = 1
    TM_DAG_MAX_ACTIVE_TASKS= 16

with DAG(
    dag_id="transfermarkt",
    description="Transfermarkt ELT data pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["ELT", "gcp", "bigquery", "cloud_storage", "dbt", ENV_NAME],
    catchup=False,
    max_active_runs=TM_DAG_MAX_ACTIVE_RUNS,
    max_active_tasks=TM_DAG_MAX_ACTIVE_TASKS
):
    PROJECT_NAME = "transfermarkt"
    GCS_BUCKET_NAME = os.getenv("BUCKET_NAME")
    GCP_CONN_ID = os.getenv("GCP_CONN_ID")
    DATASET_DIR = os.getenv("DATASET_DIR")

    start = EmptyOperator(task_id="start")

    """
    [Extraction] 
    Task to download data from Kaggle API in CSV format.
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
    Task group for preprocessing data.
    1. Excludes corrupt columns from game_lineups.csv file.
    2. Converts CSV files to Parquet format.
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

        convert_to_parquet = PythonOperator(
            task_id="convert_to_parquet",
            python_callable=t_transformer.csv_to_parquet
        )

        exclude_columns_task >> convert_to_parquet

    """ 
    [Load]
    Task group to upload files to Google Cloud Storage (GCS) and load them into BigQuery (BQ).
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
            Task to upload Parquet files to Google Cloud Storage.
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
            Task to load data from GCS into BigQuery as native tables for transformation and analysis.
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
                location=os.getenv("REGION"),
                result_timeout=60
            )

            upload_to_gcs >> load_dataset_table

    """
    Task to clear redundant local files after loading to BigQuery.
    """    
    bash_remove_command = f"rm -rfv {t_transformer.PROJECT_DATASET_DIR}"
    clean_local_dataset = BashOperator(
        task_id="clean_local_dataset",
        bash_command=bash_remove_command
    )

    """
    [Transform]
    Task group for DBT transformation tasks.
    """
    DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
    DBT_EXE_PATH = os.getenv("DBT_EXE_PATH")

    PROJECT_CONFIG = ProjectConfig(
        dbt_project_path=DBT_PROJECT_DIR
    )

    PROFILE_CONFIG = ProfileConfig(
        profile_name="default",
        target_name=ENV_NAME,
        profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml"
    )

    EXECUTION_CONFIG = ExecutionConfig(
        dbt_executable_path=DBT_EXE_PATH,
    )

    RENDER_CONFIG = RenderConfig(
        emit_datasets=False,
        test_behavior=TestBehavior.AFTER_EACH,
    )

    dbt_run_models = DbtTaskGroup(
        group_id="dbt_run_models",
        operator_args={"install_deps": True},
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RENDER_CONFIG
    )

    """
    END
    """
    end = EmptyOperator(task_id="end")

    # Define task dependencies
    start >> extract_raw_data >> pre_process_data >> load_raw_data 
    load_raw_data >> clean_local_dataset >> dbt_run_models >> end
