import os
import logging
import schemas
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from pyarrow import fs
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import bigquery


def download_dataset():
    dataset_name= "davidcariboo/player-scores"
    destination_dir= "../dataset/raw/"

    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset_name, path=destination_dir, unzip=True)


def define_schema(file_name: str) -> pd.DataFrame:
    path = f'../dataset/raw/{file_name.lower()}.csv'
    schema_class = getattr(schemas, file_name)

    logging.info(f'Setting schema for {file_name}')
    return pd.read_csv(path, dtype=schema_class.schema,
                       parse_dates=schema_class.date_cols)


def define_schema_and_upload_to_gcs():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../.creds/gcp_key.json"

    target_fs = fs.GcsFileSystem()
    bucket_name = 'transfermarkt-project'

    for file_name in schemas.class_names:
        arrow_table = pa.Table.from_pandas(define_schema(file_name))
        logging.info(f'Set schema of {file_name} and parsed to Arrow table')

        current_date = datetime.now().strftime('%m-%Y')
        gcs_path = f'{bucket_name}/{current_date}/{file_name.lower()}.parquet'

        with target_fs.open_output_stream(gcs_path) as out:
            logging.info(f'Opened GCS output stream with path: {gcs_path}')
            try:
                pq.write_table(arrow_table, out)
                logging.info(f'Wrote file {file_name.lower()}.parquet\n')
            except Exception as e:
                logging.error(f'Couldn\'t write to {gcs_path}')


def load_data_gcs_to_bq():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../.creds/gcp_key.json"
    client = bigquery.Client()
    date = datetime.now().strftime('%m-%Y')
    
    for name in schemas.class_names:
        name = name.lower()
        query = f"""
            LOAD DATA OVERWRITE final_project.{name}
            FROM FILES (
            format = 'PARQUET',
            uris = ['gs://transfermarkt-project/{date}/{name}.parquet']);
        """

        logging.info(f'Loading {name} table')
        try:
            client.query(query)
            logging.info(f'Successfully loaded {name} table\n')
        except TypeError as e:
            logging.error(e)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        datefmt='%m-%d %H:%M',
                        format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('app.log'), logging.StreamHandler()])
    logging.info("START")
    # define_schema_and_upload_to_gcs()
    # load_data_gcs_to_bq()
    logging.info('Task completed\n\n')

