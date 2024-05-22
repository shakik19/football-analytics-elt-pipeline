import os
import logging
from kaggle.api.kaggle_api_extended import KaggleApi


class DatasetDownloader:
    def __init__(self):
        self.dataset_dir = os.environ.get("DATASET_DIR")
        self.logger = logging.getLogger(__name__)

    def download_dataset(self):
        dataset_name = "davidcariboo/player-scores"
        destination_dir = f"{self.dataset_dir}/csv"

        api = KaggleApi()

        self.logger.info("Downloading raw dataset from Kaggle...")
        api.dataset_download_files(dataset_name, path=destination_dir, unzip=True)
        self.logger.info("Download completed")

