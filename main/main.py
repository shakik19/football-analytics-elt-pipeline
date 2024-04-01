import os
import logging
import schemas
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs
from kaggle.api.kaggle_api_extended import KaggleApi

def download_dataset():
    dataset_name= "davidcariboo/player-scores"
    destination_dir= "../dataset/raw/"
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset_name, path=destination_dir, unzip=True)


def define_schema(file_name: str) -> pd.DataFrame:
    path = f'../dataset/raw/{file_name.lower()}.csv'
    schema_class = getattr(schemas, file_name)
    logging.info(f'📃 Setting schema for {file_name}')
    return pd.read_csv(path, dtype=schema_class.schema,
                       parse_dates=schema_class.date_cols)


def define_schema_and_upload_to_gcs():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../.creds/gcp_key.json"
    csv_file_names = [
                    'Appearances',
                    'Club_Games',
                    'Clubs',
                    'Competitions',
                    'Game_Events',
                    'Game_Lineups',
                    'Games',
                    'Player_Valuations',
                    'Players']

    target_fs = fs.GcsFileSystem()
    bucket_name = 'transfermarkt-project'

    for file_name in csv_file_names:
        arrow_table = pa.Table.from_pandas(define_schema(file_name))
        logging.info(f'👉 Set schema of {file_name} and parsed to Arrow table')

        gcs_path = f'{bucket_name}/final/{file_name.lower()}.parquet'
        with target_fs.open_output_stream(gcs_path) as out:
            logging.info(f'📂 Opened GCS output stream with path: {gcs_path}')
            try:
                pq.write_table(arrow_table, out)
                logging.info(f'✅ Wrote file {file_name.lower()}.parquet\n')
            except Exception as e:
                logging.error(f'❌ Couldn\'t write {gcs_path}')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        datefmt='%m-%d %H:%M',
                        format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('app.log'), logging.StreamHandler()])
    logging.info("🔰 START")
    define_schema_and_upload_to_gcs()
    logging.info('Task completed 🔚\n\n')

