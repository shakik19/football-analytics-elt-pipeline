PROJECT_ID = "fast-forward-412713"
BUCKET_NAME = "transfermarkt-data"
SEED_DATASET_NAME = "transfermarkt_seed"
CORE_DATASET_NAME = "transfermarkt_core"

SERVICE_ACC_PATH = "../credentials/gcp_key.json"

RAW_DATASET_DIR = "./dataset/raw"
PARQUET_DIR = "./dataset/parquet"
DBT_PROJECT_DIR = "../dbt"

tables = [
        'Appearances',
        'Club_games',
        'Clubs',
        'Competitions',
        'Game_events',
        'Game_lineups',
        'Games',
        'Player_valuations',
        'Players']