import os
import logging
import threading
import pandas as pd
import time
from pyarrow import csv, parquet as pq
from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud.storage import Client, transfer_manager
from google.cloud import bigquery

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    datefmt='%m-%d %H:%M',
                    format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('pipeline.log'), logging.StreamHandler()])
DATASET_DIR = "../dataset"
tables = [
    'Appearances',
    'Club_games',
    'Clubs',
    'Competitions',
    'Game_events',
    'Game_lineups',
    'Games',
    'Player_valuations',
    'Players']


def download_dataset():
    os.environ["KAGGLE_CONFIG_DIR"] = os.getenv("KAGGLE_API_KEY")
    dataset_name = "davidcariboo/player-scores"
    destination_dir = f"{DATASET_DIR}/csv"

    api = KaggleApi()

    logging.info("Downloading raw dataset from Kaggle...")
    api.dataset_download_files(dataset_name, path=destination_dir, unzip=True)
    logging.info("Download completed")

    exclude_corrupt_columns()


def exclude_corrupt_columns():
    # Excluding corrupted columns in the dataset
    # The schema is inconsistent in the "number" and "team_captain columns
    game_lineups_path = "../dataset/csv/game_lineups.csv"
    df = pd.read_csv(game_lineups_path)
    df = df.drop(["number", "team_captain"], axis=1)
    logging.info("Removed corrupted columns from game_lineups.csv")
    df.to_csv(game_lineups_path, index=False)
    logging.info("Saved game_lineups.csv")


def process_csv_to_parquet(filename: str):
    csv_path = f'{DATASET_DIR}/csv/{filename}.csv'
    parquet_path = f'{DATASET_DIR}/parquet/{filename}.parquet'

    arrow_table = csv.read_csv(csv_path, csv.ReadOptions(bloteam_captainck_size=100000))
    logging.info(f'Parsed {filename}.csv to Arrow table')

    pq.write_table(arrow_table, parquet_path)
    logging.info(f'Saved {filename}.parquet to ./dataset/parquet')


def csv_to_parquet():
    threads = []
    start_time = time.perf_counter()

    for filename in tables:
        thread = threading.Thread(target=process_csv_to_parquet, args=[filename.lower()])
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    end_time = time.perf_counter()
    logging.info(f"Finished  csv_to_parquet job. Took: {(end_time - start_time):2f}s")


def upload_to_gcs():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_ACC_PATH")
    bucket_name = os.getenv("BUCKET_NAME")
    filenames = [f"{filename.lower()}.parquet" for filename in tables]
    dataset_dir = os.getenv("DATASET_DIR")
    source_dir = f"{dataset_dir}/parquet"

    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)

    start = time.perf_counter()
    results = transfer_manager.upload_many_from_filenames(
        bucket,
        filenames,
        source_directory=source_dir,
        worker_type=transfer_manager.PROCESS,
        max_workers=6,
    )
    end = time.perf_counter()
    logging.info(f"Took: {(end-start):.2f}")

    for name, result in zip(filenames, results):
        if isinstance(result, Exception):
            logging.error("Failed to upload {} due to exception: {}".format(name, result))
        else:
            logging.info("Uploaded {} to {}.".format(name, bucket.name))


def load_bigquery_seed_dataset():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_ACC_PATH")
    dataset_name = os.getenv("SEED_DATASET_NAME")
    bucket_name = os.getenv("BUCKET_NAME")
    client = bigquery.Client()

    for table_name in tables:
        query = f"""
            CREATE OR REPLACE TABLE {dataset_name}.{table_name}
            OPTIONS (
            format = 'PARQUET',
            uris = ['gs://{bucket_name}/raw_data/{table_name}.parquet']
            );
        """
        try:
            client.query_and_wait(query)
            logging.info(f'Successfully loaded {table_name} table\n')
        except TypeError as e:
            logging.error(e)


def clean_dirs():
    directories = [f"{DATASET_DIR}/csv", f"{DATASET_DIR}/parquet"]

    logging.info("Cleaning raw csv files")
    for dir_path in directories:
        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)

            if os.path.isfile(file_path):
                os.remove(file_path)
                logging.info(f"Deleted {file_path}")


if __name__ == "__main__":
    # download_dataset()
    # csv_to_parquet()
    # upload_to_gcs()
    clean_dirs()