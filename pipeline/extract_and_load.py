import variables as pvars
import schema
import os
import logging
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
from google.cloud.storage import Client, transfer_manager
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import bigquery, storage

logging.basicConfig(level=logging.INFO,
                    datefmt='%m-%d %H:%M',
                    format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('app.log'), logging.StreamHandler()])


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

def write_to_parquet():
    for file_name in pvars.tables:
        table = pa.Table.from_pandas(define_schema(file_name=file_name))
        dest_filename = f"{pvars.PARQUET_DIR}/{file_name.lower()}.parquet"
        pq.write_table(table, dest_filename)


def upload_directory_with_transfer_manager():
    bucket_name = pvars.BUCKET_NAME
    source_directory = pvars.PARQUET_DIR

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = pvars.SERVICE_ACC_PATH
    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)

    directory_as_path_obj = Path(source_directory)
    paths = directory_as_path_obj.rglob("*")

    file_paths = [path for path in paths if path.is_file()]

    relative_paths = [path.relative_to(source_directory) for path in file_paths]

    string_paths = [str(path) for path in relative_paths]

    logging.info("Found {} files.".format(len(string_paths)))

    results = transfer_manager.upload_many_from_filenames(
        bucket, string_paths, source_directory=source_directory, max_workers=6
    )

    for name, result in zip(string_paths, results):
        if isinstance(result, Exception):
            logging.info("Failed to upload {} due to exception: {}".format(name, result))
        else:
            logging.info("Uploaded {} to {}.".format(name, bucket.name))


def create_bq_seed_dataset():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = pvars.SERVICE_ACC_PATH
    dataset_name = pvars.DATASET_NAME
    bucket_name = pvars.BUCKET_NAME
    client = bigquery.Client()
    
    for table_name in pvars.tables:
        file_path = f"gs://{bucket_name}/{table_name.lower()}.parquet"
        query = f"""
            CREATE OR REPLACE EXTERNAL TABLE `{pvars.PROJECT_ID}.{dataset_name}.{table_name.lower()}`
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


def clean_filse():
    dir_paths = [pvars.RAW_DATASET_DIR, pvars.PARQUET_DIR]

    logging.info("Cleaning csv and parquet files")
    for dir_path in dir_paths:
        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)

            if os.path.isfile(file_path):
                os.remove(file_path)
                logging.info(f"Deleted {file_path}")


if __name__ == "__main__":
    download_dataset()
    write_to_parquet()
    upload_directory_with_transfer_manager()
    create_bq_seed_dataset()
    # clean_filse()
