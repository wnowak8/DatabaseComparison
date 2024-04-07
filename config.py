import os

NUM_RECORDS = 100000
UPDATE_RECORDS = 10000
TOTAL_CONNECTIONS = 1
MONGO_DATA_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "mongo", "assets"
)
POSTGRES_DATA_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "postgres", "assets"
)
