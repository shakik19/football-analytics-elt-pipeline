import os
import logging
import time
import threading
from dotenv import load_dotenv
from google.cloud.storage import Client, transfer_manager
from google.cloud import bigquery


class DataLoader:
    def __init__(self, project_name: str):
        load_dotenv()
        self.logger = logging.getLogger(__name__)
        self.PROJECT_NAME = project_name
        self.DATASET_DIR = os.environ.get("DATASET_DIR")
        self.PARQUET_DATASET_DIR = f"{self.DATASET_DIR}/{project_name}/parquet"
        self.BUCKET_NAME = os.getenv("BUCKET_NAME")
        self.TABLES = [file.split(".")[0] for file in os.listdir(self.PARQUET_DATASET_DIR) if file.endswith(".parquet")]

    def upload_to_gcs(self):
        filenames = [f"{filename}.parquet" for filename in self.TABLES]
        source_dir = self.PARQUET_DATASET_DIR

        storage_client = Client()
        bucket = storage_client.bucket(self.BUCKET_NAME)

        start = time.perf_counter()
        self.logger.info(f"Uploading parquet directory to {self.BUCKET_NAME} bucket...")
        results = transfer_manager.upload_many_from_filenames(
            bucket,
            filenames,
            source_directory=source_dir,
            worker_type=transfer_manager.THREAD,
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
            uris = ['gs://{self.BUCKET_NAME}/{table_name}.parquet']
            );
        """

        try:
            self.logger.info(f"Sent query for {table_name} table to BQ api")
            client.query_and_wait(query)
            self.logger.info(f'Successfully loaded {table_name} table')
        except TypeError as e:
            self.logger.error(e)


    def load_bigquery_seed_dataset(self):
        threads = []
        for table in self.TABLES:
            thread =  threading.Thread(target=self.load_into_bigquery,
                                       args=[table])
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
