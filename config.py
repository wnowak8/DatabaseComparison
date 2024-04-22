import os

NUM_RECORDS = 1000
UPDATE_RECORDS = 100
TOTAL_CONNECTIONS = 1
MONGO_DATA_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "mongo", "assets_10_tys"
)
POSTGRES_DATA_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "postgres", "assets_10_tys"
)
