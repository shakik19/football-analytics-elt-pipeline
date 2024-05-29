import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from include.extractor import DatasetDownloader
from include.transformer import DataTransformer
from include.loader import DataLoader
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
    tags=["transfermarkt", "ELT", "gcp", "bigquery", "cloud_storage","dbt"],
    catchup=False
):
    PROJECT_NAME = "transfermarkt"
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
    t_loader = DataLoader(project_name=PROJECT_NAME)
    load_datalake = PythonOperator(
        task_id="load_datalake",
        python_callable=t_loader.upload_to_gcs,
    )

    """
    Loading data from Data Lake to Data Warehouse as native tables for 
    transformation and analysis
    """
    load_seed_dataset = PythonOperator(
        task_id="load_seed_dataset",
        python_callable=t_loader.load_bigquery_seed_dataset,
    )


    """
    [Transform]
    DBT transformation tasks
    """
    DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR")
    DBT_EXE_PATH = os.environ.get("DBT_EXE_PATH")

    PROJECT_CONFIG = ProjectConfig(
        dbt_project_path=DBT_PROJECT_DIR
    )

    PROFILE_CONFIG = ProfileConfig(
        profile_name=os.environ.get("DBT_PROFILES_NAME"),
        target_name=os.environ.get("DBT_PROFILES_TARGET"),
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
    clean_local_dataset = PythonOperator(
        task_id = "clean_local_dataset",
        python_callable=t_extractor.remove_all_in_path,
        op_args=[os.environ.get("DATASET_DIR")]
    )

    end = EmptyOperator(task_id="end")


    start >> extract_raw_data >> pre_process_data >> convert_to_parquet
    convert_to_parquet >> load_datalake >> load_seed_dataset
    load_seed_dataset >> dbt_run_models >> clean_local_dataset >> end