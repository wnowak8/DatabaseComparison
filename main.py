import os
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from app.mongo import MongoDB
from app.postgres import PostgresDB
from app.select import get_all_data_postgres_with_performance_metrics, get_initial_performance_metrics, get_windows_performance_metrics

PATH_TO_DATA = os.path.join(os.getcwd(), "data")
file_path_1 = os.path.join(PATH_TO_DATA, "Cell_Phones_and_Accessories.csv")
file_path_2 = os.path.join(PATH_TO_DATA, "Sports_and_Outdoors.csv")

logging.basicConfig(
    level=getattr(logging, "INFO"),
    format="%(asctime)s.%(msecs)03d-%(levelname)s-%(funcName)s()-%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

postgresDB = PostgresDB(db_host=os.environ.get("POSTGRES_HOST"),
                                   db_port=os.environ.get("POSTGRES_PORT"),
                                   db_user=os.environ.get("POSTGRES_USER"),
                                   db_password=os.environ.get("POSTGRES_PASSWORD"),
                                   db_name=os.environ.get("POSTGRES_DATABASE"))

def dateparse (time_in_secs):    
    return datetime.fromtimestamp(float(time_in_secs))

def send_chunks_to_database(database, table_collection_name, file_path):
    chunk_size = 1000  # Możesz dostosować rozmiar chunka do swoich potrzeb
    reader = pd.read_csv(file_path,
                         chunksize=chunk_size,
                         decimal='.',
                         parse_dates=['date'],
                         date_format='%Y-%m-%d %H:%M:%S.%f',
                         index_col='date')

    for i, chunk in enumerate(reader):
        chunk = chunk.replace({',': '.'}, regex=True)
        database.send_df_to_db(table_collection_name, chunk)
        logging.info(f"Processed chunk {i+1}")

def run_postgres():

    


    send_chunks_to_database(postgresDB, 'reviews', file_path_1)
    send_chunks_to_database(postgresDB, 'reviews', file_path_2)

def run_mongodb():
    mongodb = MongoDB(db_host=os.environ.get("MONGODB_HOST"),
                                db_port=os.environ.get("MONGODB_PORT"),
                                db_user=os.environ.get("MONGODB_USER"),
                                db_password=os.environ.get("MONGODB_PASSWORD"),
                                db_name=os.environ.get("MONGODB_DATABASE"))

    send_chunks_to_database(mongodb, 'reviews', file_path_1)
    send_chunks_to_database(mongodb, 'reviews', file_path_2)

if __name__ == '__main__':
    # run_postgres()
    run_mongodb()
    # metrics = get_windows_performance_metrics()
    # get_initial_performance_metrics()
    # get_all_data_postgres_with_performance_metrics(table_name='reviews', postgresDB=postgresDB)