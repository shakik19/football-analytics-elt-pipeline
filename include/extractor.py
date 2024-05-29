import os
import shutil
import logging
from kaggle.api.kaggle_api_extended import KaggleApi


class DatasetDownloader:
    def __init__(self, project_name: str):
        self.PROJECT_NAME = project_name
        self.DATASET_DIR = os.environ.get("DATASET_DIR")
        self.create_required_dirs()
        self.logger = logging.getLogger(__name__)
    

    def download_dataset(self, dataset_name: str):
        destination_dir = f"{self.DATASET_DIR}/{self.PROJECT_NAME}/csv"

        api = KaggleApi()

        self.logger.info("Downloading raw dataset from Kaggle...")
        api.dataset_download_files(dataset_name, path=destination_dir, unzip=True)
        self.logger.info("Download completed")

    def create_required_dirs(self):
        project_dataset_dir = f"{self.DATASET_DIR}/{self.PROJECT_NAME}"
        parquet_dir = f"{project_dataset_dir}/parquet"
        csv_dir = f"{project_dataset_dir}/csv"   
        directories = [self.DATASET_DIR, project_dataset_dir, parquet_dir, csv_dir]

        for directory in directories:
            if not os.path.exists(directory):
                os.mkdir(directory)

    def remove_all_in_path(self, dataset_dir: str):
        try:
        # Check if the path exists
            if os.path.exists(dataset_dir):
                # Recursively remove the directory and all its contents
                shutil.rmtree(dataset_dir)
                self.logger.info(f"All files and directories in '{dataset_dir}' have been removed.")
            else:
                self.logger.info(f"The path '{dataset_dir}' does not exist.")
        except PermissionError:
            self.logger.warn(f"Permission denied to modify the path '{dataset_dir}'.")
        except Exception as e:
            self.logger.warn(f"An error occurred: {e}")
