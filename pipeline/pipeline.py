import os
import logging
import threading
import pandas as pd
import time
from typing import Callable
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
    'appearances',
    'game_lineups',
    'game_events',
    'club_games',
    'clubs',
    'competitions',
    'games',
    'player_valuations',
    'players']


def download_dataset():
    os.environ["KAGGLE_CONFIG_DIR"] = os.getenv("KAGGLE_API_KEY")
    dataset_name = "davidcariboo/player-scores"
    destination_dir = f"{DATASET_DIR}/csv"

    api = KaggleApi()

    logging.info("Downloading raw dataset from Kaggle...")
    api.dataset_download_files(dataset_name, path=destination_dir, unzip=True)
    logging.info("Download completed")

    exclude_corrupt_columns()


def run_using_threads(function_obj: Callable[[], None]):
    threads = []
    start_time = time.perf_counter()

    for filename in tables:
        thread = threading.Thread(target=function_obj, args=[filename])
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    end_time = time.perf_counter()
    logging.info(f"Finished {function_obj.__name__} job")
    logging.info(f"Took: {(end_time - start_time):.2f}s")


def exclude_corrupt_columns():
    # Excluding corrupted columns in the dataset
    # The schema is inconsistent in the "number" and "team_captain columns
    game_lineups_path = "../dataset/csv/game_lineups.csv"
    df = pd.read_csv(game_lineups_path)
    df = df.drop(["number", "team_captain"], axis=1)
    logging.info("Removed corrupted columns from game_lineups.csv")
    df.to_csv(game_lineups_path, index=False)
    logging.info("Saved game_lineups.csv\n")


def process_csv_to_parquet(filename: str):
    csv_path = f'{DATASET_DIR}/csv/{filename}.csv'
    parquet_path = f'{DATASET_DIR}/parquet/{filename}.parquet'

    arrow_table = csv.read_csv(csv_path, csv.ReadOptions(block_size=100000))
    logging.info(f'Parsed {filename}.csv to Arrow table')

    pq.write_table(arrow_table, parquet_path)
    logging.info(f'Saved {filename}.parquet to ./dataset/parquet')


def csv_to_parquet():
    run_using_threads(process_csv_to_parquet)


def upload_to_gcs():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_ACC_PATH")
    bucket_name = os.getenv("BUCKET_NAME")
    filenames = [f"{filename}.parquet" for filename in tables]
    source_dir = f"{DATASET_DIR}/parquet"

    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)

    start = time.perf_counter()
    logging.info(f"Uploading parquet directory to {bucket_name} bucket...")
    results = transfer_manager.upload_many_from_filenames(
        bucket,
        filenames,
        source_directory=source_dir,
        worker_type=transfer_manager.PROCESS,
        max_workers=6,
    )
    end = time.perf_counter()

    for name, result in zip(filenames, results):
        if isinstance(result, Exception):
            logging.error("Failed to upload {} due to exception: {}".format(name, result))
        else:
            logging.info("Uploaded {} to {}.".format(name, bucket.name))
    logging.info(f"Finished upload_to_gcs job")
    logging.info(f"Took: {(end - start):.2f}s")


def load_dataset(table_name: str):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_ACC_PATH")
    project_id = os.getenv("PROJECT_ID")
    dataset_name = os.getenv("SEED_DATASET_NAME")
    bucket_name = os.getenv("BUCKET_NAME")
    client = bigquery.Client()

    query = f"""
        LOAD DATA OVERWRITE `{project_id}.{dataset_name}.{table_name}`
        FROM FILES (
        format = 'PARQUET',
        uris = ['gs://{bucket_name}/{table_name}.parquet']
        );
    """

    try:
        logging.info(f"Sent query for {table_name} table to BQ api")
        client.query_and_wait(query)
        logging.info(f'Successfully loaded {table_name} table')
    except TypeError as e:
        logging.error(e)


def load_bigquery_seed_dataset():
    run_using_threads(load_dataset)


def clean_dirs():
    directories = [f"{DATASET_DIR}/csv", f"{DATASET_DIR}/parquet"]

    logging.info("Cleaning raw csv files")
    for dir_path in directories:
        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)

            if os.path.isfile(file_path):
                os.remove(file_path)
                logging.info(f"Deleted {file_path}")

