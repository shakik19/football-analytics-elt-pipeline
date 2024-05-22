import os
import logging
import threading
import pandas as pd
from pyarrow import csv, parquet as pq
from dotenv import load_dotenv


class DataTransformer:
    def __init__(self):
        self.DATASET_DIR = os.environ.get("DATASET_DIR")
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
            'players'
        ]


    def exclude_corrupt_columns(self):
        # Excluding corrupted columns in the dataset
        # The schema is inconsistent in the "number" and "team_captain columns
        game_lineups_path = f"{self.DATASET_DIR}/csv/game_lineups.csv"
        df = pd.read_csv(game_lineups_path)
        if set(["number", "team_captain"]).issubset(df.columns):
            df = df.drop(["number", "team_captain"], axis=1)
            self.logger.info("Removed corrupted columns from game_lineups.csv")
            df.to_csv(game_lineups_path, index=False)
            self.logger.info("Saved game_lineups.csv\n")
        else:
            self.logger.info("No corrupt column exists")


    def process_csv_to_parquet(self, filename: str):
        csv_path = f"{self.DATASET_DIR}/csv/{filename}.csv"
        parquet_path = f"{self.DATASET_DIR}/parquet/{filename}.parquet"

        arrow_table = csv.read_csv(csv_path, csv.ReadOptions(block_size=100000))
        self.logger.info(f"Parsed {filename}.csv to Arrow table")

        pq.write_table(arrow_table, parquet_path)
        self.logger.info(f"Saved {filename}.parquet to {self.DATASET_DIR}/parquet")


    def csv_to_parquet(self):
        threads = []
        for table in self.TABLES:
            self.logger.info("Starting task")
            thread =  threading.Thread(target=self.process_csv_to_parquet,
                                       args=[table])
            threads.append(thread)
            thread.start()
            self.logger.info(f"{table} thread started")           
        for thread in threads:
            self.logger.info(f"{table} thread joined")           
            thread.join()
