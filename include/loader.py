import os
import logging
import time
from dotenv import load_dotenv
from google.cloud.storage import Client, transfer_manager
from google.cloud import bigquery
from helper import PipelineHelper


class DataLoader:
    def __init__(self):
        load_dotenv()
        self.logger = logging.getLogger(__name__)
        self.TABLES = [
            'appearances',
            'game_lineups',
            'game_events',
            'club_games',
            'clubs',
            'competitions',
            'games',
            'player_valuations',
            'players']
        self.DATASET_DIR = "../dataset"
        self.google_application_credentials = os.getenv("SERVICE_ACC_PATH")
        self.bucket_name = os.getenv("BUCKET_NAME")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_application_credentials

    def upload_to_gcs(self):
        filenames = [f"{filename}.parquet" for filename in self.TABLES]
        source_dir = f"{self.DATASET_DIR}/parquet"

        storage_client = Client()
        bucket = storage_client.bucket(self.bucket_name)

        start = time.perf_counter()
        self.logger.info(f"Uploading parquet directory to {self.bucket_name} bucket...")
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
                self.logger.error("Failed to upload {} due to exception: {}".format(name, result))
            else:
                self.logger.info("Uploaded {} to {}.".format(name, bucket.name))
        self.logger.info(f"Finished upload_to_gcs job")
        self.logger.info(f"Took: {(end - start):.2f}s")


    def load_into_bigquery(self, table_name: str):
        project_id = os.getenv("PROJECT_ID")
        dataset_name = os.getenv("SEED_DATASET_NAME")
        client = bigquery.Client()

        query = f"""
            LOAD DATA OVERWRITE `{project_id}.{dataset_name}.{table_name}`
            FROM FILES (
            format = 'PARQUET',
            uris = ['gs://{self.bucket_name}/{table_name}.parquet']
            );
        """

        try:
            self.logger.info(f"Sent query for {table_name} table to BQ api")
            client.query_and_wait(query)
            self.logger.info(f'Successfully loaded {table_name} table')
        except TypeError as e:
            self.logger.error(e)

    def load_bigquery_seed_dataset(self):
        helper = PipelineHelper()
        helper.run_using_threads(self.TABLES, self.load_into_bigquery)
