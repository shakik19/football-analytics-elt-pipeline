import variables
import os
import logging
import pyarrow.parquet as pq
from pyarrow import fs, csv
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO,
                    datefmt='%m-%d %H:%M',
                    format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('app.log'), logging.StreamHandler()])


def download_dataset():
    dataset_name= "davidcariboo/player-scores"
    destination_dir= variables.RAW_DATASET_DIR

    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset_name, path=destination_dir, unzip=True)


def upload_to_gcs():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = variables.SERVICE_ACC_PATH

    target_fs = fs.GcsFileSystem()
    bucket_name = variables.BUCKET_NAME

    for file_name in variables.tables:
        path = f'{variables.RAW_DATASET_DIR}{file_name}.csv'
        arrow_table = csv.read_csv(path,
                                   csv.ReadOptions(use_threads=True,
                                                   block_size=1000))
        logging.info(f'Parsed {file_name} to Arrow table')

        gcs_path = f'{bucket_name}/raw/{file_name.lower()}.parquet'

        with target_fs.open_output_stream(gcs_path) as out:
            logging.info(f'Opened GCS output stream with path: gs://{gcs_path}')
            try:
                pq.write_table(arrow_table, out)
                logging.info(f'Wrote file {file_name.lower()}.parquet\n')
            except Exception as e:
                logging.error(f'Couldn\'t write to {gcs_path}')


def create_bq_seed_dataset():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = variables.SERVICE_ACC_PATH
    dataset_name = variables.DATASET_NAME
    bucket_name = variables.BUCKET_NAME
    client = bigquery.Client()
    
    for table_name in variables.tables:
        query = f"""
            CREATE OR REPLACE EXTERNAL TABLE {dataset_name}.{table_name}
            OPTIONS (
            format = 'PARQUET',
            uris = ['gs://{bucket_name}/raw/{table_name}.parquet']
            );
        """

        logging.info(f'Loading {table_name} table')
        try:
            client.query_and_wait(query)
            logging.info(f'Successfully loaded {table_name} table\n')
        except TypeError as e:
            logging.error(e)


def clean_csvs():
    dir_path = variables.RAW_DATASET_DIR

    logging.info("Cleaning raw csv files")
    for filename in os.listdir(dir_path):
        file_path = os.path.join(dir_path, filename)

        if os.path.isfile(file_path):
            os.remove(file_path)
            logging.info(f"Deleted {file_path}")
