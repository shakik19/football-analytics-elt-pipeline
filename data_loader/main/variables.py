PROJECT_NAME = ""
BUCKET_NAME = "transfermarkt-data-storage"
DATASET_NAME = "transfermarkt_seed"

SERVICE_ACC_PATH = "../creds/gcp_key.json"

RAW_DATASET_DIR = "../dataset/raw/"

tables = [
        'appearances',
        'club_games',
        'clubs',
        'competitions',
        'game_events',
        'game_lineups',
        'games',
        'player_valuations',
        'players']