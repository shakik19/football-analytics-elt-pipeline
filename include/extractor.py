import os
import logging
from kaggle.api.kaggle_api_extended import KaggleApi


class DatasetDownloader:
    def __init__(self, project_name: str):
        self.project_name = project_name
        self.dataset_dir = os.environ.get("DATASET_DIR")
        self.logger = logging.getLogger(__name__)
    

    def download_dataset(self, dataset_name: str):
        destination_dir = f"{self.dataset_dir}/{self.project_name}/csv"

        api = KaggleApi()

        self.logger.info("Downloading raw dataset from Kaggle...")
        api.dataset_download_files(dataset_name, path=destination_dir, unzip=True)
        self.logger.info("Download completed")

