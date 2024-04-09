import variables as pvars
import schema
import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pyarrow import fs
from kaggle.api.kaggle_api_extended import KaggleApi
from prefect_gcp import GcpCredentials
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect_dbt.cli import DbtCoreOperation


logging = get_run_logger()


@task(name="extract-data", retries=2, log_prints=True)
def download_dataset():
    dataset_name= "davidcariboo/player-scores"
    destination_dir= pvars.RAW_DATASET_DIR

    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset_name, path=destination_dir, unzip=True)


def define_schema(file_name: str) -> pd.DataFrame:
    path = f'{pvars.RAW_DATASET_DIR}/{file_name.lower()}.csv'
    schema_class = getattr(schema, file_name)
    logging.info(f'Setting schema for {file_name}')
    return pd.read_csv(path, dtype=schema_class.schema)


@task(name="process-load-datalake", retries=2, log_prints=True)
def process_and_load_datalake():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = pvars.SERVICE_ACC_PATH

    target_fs = fs.GcsFileSystem()
    bucket_name = pvars.BUCKET_NAME

    for file_name in pvars.tables:
        table = pa.Table.from_pandas(define_schema(file_name=file_name))
        logging.info(f'Parsed {file_name} to Arrow table')
        
        gcs_path = f'{bucket_name}/{file_name.lower()}.parquet'

        with target_fs.open_output_stream(gcs_path) as out:
            logging.info(f'Opened GCS output stream with path: gs://{gcs_path}')
            try:
                pq.write_table(table, out)
                logging.info(f'Wrote file {file_name.lower()}.parquet\n')
            except Exception as e:
                logging.error(f'Couldn\'t write to {gcs_path}')



@task(name="load-warehouse", retries=1, log_prints=True)
def create_bq_seed_dataset():
    client = GcpCredentials.load("gcp-key").get_bigquery_client()
    dataset_id = pvars.SEED_DATASET_NAME
    project_id = pvars.PROJECT_ID
    bucket_name = pvars.BUCKET_NAME
    
    for table_name in pvars.tables:
        file_path = f"gs://{bucket_name}/{table_name.lower()}.parquet"
        query = f"""
            CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{dataset_id}.{table_name.lower()}`
            OPTIONS (
            format = 'PARQUET',
            uris = ['{file_path}']
            );
        """

        logging.info(f'Loading {table_name} table')
        try:
            client.query_and_wait(query)
            logging.info(f'Successfully loaded {table_name} table\n')
        except TypeError as e:
            logging.error(e)


@task(name="run-dbt-models")
def trigger_dbt_flow() -> str:
    result = DbtCoreOperation(
        commands=["dbt debug", "dbt run"],
        project_dir=pvars.DBT_PROJECT_DIR,
        profiles_dir=pvars.DBT_PROJECT_DIR
    ).run()
    return result


@task(name="clean-local", retries=2, log_prints=True)
def clean_filse():
    dir_paths = [pvars.RAW_DATASET_DIR, pvars.PARQUET_DIR]

    logging.info("Cleaning csv and parquet files")
    for dir_path in dir_paths:
        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)

            if os.path.isfile(file_path):
                os.remove(file_path)
                logging.info(f"Deleted {file_path}")


@flow(name="main-flow", log_prints=True)
def main_flow():
    download_dataset()
    process_and_load_datalake()
    create_bq_seed_dataset()
    trigger_dbt_flow()
    # clean_filse()


if __name__ == "__main__":
    main_flow()
