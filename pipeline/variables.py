PROJECT_ID = "fast-forward-412713"
BUCKET_NAME = "transfermarkt-data"
DATASET_NAME = "transfermarkt_seed"

SERVICE_ACC_PATH = "./creds/gcp_key.json"

RAW_DATASET_DIR = "./dataset/raw"
PARQUET_DIR = "./dataset/parquet"

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