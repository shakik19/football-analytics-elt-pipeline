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


"""
Function: download_dataset

Description:
This function downloads files from a Kaggle dataset and extracts them to a specified directory.

Behavior:
1. Specifies the name of the Kaggle dataset and the destination directory.
2. Authenticates the Kaggle API.
3. Downloads files from the specified dataset to the destination directory.
4. Extracts the downloaded files if they are zipped.
"""

def download_dataset():
    dataset_name= "davidcariboo/player-scores"
    destination_dir= "../dataset/raw/"

    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset_name, path=destination_dir, unzip=True)


"""
Function: define_schema

Description:
This function defines a schema for a given CSV file and returns a pandas DataFrame with the specified schema.

Parameters:
- file_name (str): Name of the CSV file.
  
Returns:
- pd.DataFrame: DataFrame with the defined schema.
"""

def define_schema(file_name: str) -> pd.DataFrame:
    path = f'../dataset/raw/{file_name.lower()}.csv'
    schema_class = getattr(schemas, file_name)

    logging.info(f'Setting schema for {file_name}')
    return pd.read_csv(path, dtype=schema_class.schema,
                       parse_dates=schema_class.date_cols)


"""
Function: define_schema_and_upload_to_gcs

Description:
This function sets schemas for CSV files, converts them to Arrow tables, and uploads them to Google Cloud Storage (GCS) in Parquet format.

Behavior:
1. Sets Google Cloud credentials.
2. Iterates through CSV files defined in the schema class.
3. Converts CSV data to an Arrow table with the defined schema.
4. Uploads the Arrow table from memory to GCS in Parquet format under a specified path.
"""

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


"""
Function: load_data_gcs_to_bq

Description:
This function loads data from Google Cloud Storage (GCS) into BigQuery tables.

Behavior:
1. Sets Google Cloud credentials.
2. Initializes a BigQuery client.
3. Retrieves the current date.
4. Iterates through the class names defined in the schema.
5. Constructs a SQL query to load data from GCS into BigQuery tables.
6. Executes the SQL query to load data into BigQuery.
"""

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

