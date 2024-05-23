from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from include.extractor import DatasetDownloader
from include.transformer import DataTransformer
from include.loader import DataLoader


with DAG(
    dag_id="transfermarkt",
    description="Transfermarkt ELT data pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    tags=["elt", "gcp", "dbt"],
    catchup=False
):
    start = EmptyOperator(task_id="start")

    """
    [Extraction] 
    Get data from kaggle api in csv format 
    """
    t_extractor = DatasetDownloader()
    extract_dataset = PythonOperator(
        task_id="extract_dataset",
        python_callable=t_extractor.download_dataset
    )


    """
    [Pre-transformation]
    Excluding some corrupt columns from the game_lineups.csv file
    """
    t_transformer = DataTransformer()
    pre_process_data = PythonOperator(
        task_id="pre_process_data",
        python_callable=t_transformer.exclude_corrupt_columns
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
    t_loader = DataLoader()
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




    end = EmptyOperator(task_id="end")

    start >> extract_dataset >> pre_process_data >> convert_to_parquet
    convert_to_parquet >> load_datalake >> load_seed_dataset >> end