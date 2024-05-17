import os
import logging
from dotenv import load_dotenv
from transformer import DataTransformer
from kaggle.api.kaggle_api_extended import KaggleApi


class DatasetDownloader:
    def __init__(self, dataset_dir="../dataset"):
        self.dataset_dir = dataset_dir
        load_dotenv()
        self.logger = logging.getLogger(__name__)

    def download_dataset(self):
        os.environ["KAGGLE_CONFIG_DIR"] = os.getenv("KAGGLE_API_KEY")
        dataset_name = "davidcariboo/player-scores"
        destination_dir = f"{self.dataset_dir}/csv"

        api = KaggleApi()

        self.logger.info("Downloading raw dataset from Kaggle...")
        api.dataset_download_files(dataset_name, path=destination_dir, unzip=True)
        self.logger.info("Download completed")

        transformer = DataTransformer()
        transformer.exclude_corrupt_columns()
