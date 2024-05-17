import logging
import pandas as pd
from pyarrow import csv, parquet as pq
from dotenv import load_dotenv
from helper import PipelineHelper


class DataTransformer:
    def __init__(self, dataset_dir="../dataset"):
        self.dataset_dir = dataset_dir
        load_dotenv()
        self.logger = logging.getLogger(__name__)
        self.tables = [
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
        game_lineups_path = f"{self.dataset_dir}/csv/game_lineups.csv"
        df = pd.read_csv(game_lineups_path)
        df = df.drop(["number", "team_captain"], axis=1)
        self.logger.info("Removed corrupted columns from game_lineups.csv")
        df.to_csv(game_lineups_path, index=False)
        self.logger.info("Saved game_lineups.csv\n")


    def process_csv_to_parquet(self, filename: str):
        csv_path = f"{self.dataset_dir}/csv/{filename}.csv"
        parquet_path = f"{self.dataset_dir}/parquet/{filename}.parquet"

        arrow_table = csv.read_csv(csv_path, csv.ReadOptions(block_size=100000))
        self.logger.info(f"Parsed {filename}.csv to Arrow table")

        pq.write_table(arrow_table, parquet_path)
        self.logger.info(f"Saved {filename}.parquet to {self.dataset_dir}/parquet")


    def csv_to_parquet(self):
        helper = PipelineHelper()
        helper.run_using_threads(self.tables, self.process_csv_to_parquet)
