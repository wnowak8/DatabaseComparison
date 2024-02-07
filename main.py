import os
import pandas as pd
from app.mongo import MongoDB

from app.postgres import PostgresDB

PATH_TO_DATA = os.path.join(os.getcwd(), "data")
file_path_1 = os.path.join(PATH_TO_DATA, "Cell_Phones_and_Accessories.csv")
file_path_2 = os.path.join(PATH_TO_DATA, "Sports_and_Outdoors.csv")


def run_postgres():
    postgresDB = PostgresDB(db_host=os.environ.get("POSTGRES_HOST"),
                            db_port=os.environ.get("POSTGRES_PORT"),
                            db_user=os.environ.get("POSTGRES_USER"),
                            db_password=os.environ.get("POSTGRES_PASSWORD"),
                            db_name=os.environ.get("POSTGRES_DATABASE"))

    df_1 = pd.read_csv(file_path_1)
    df_2 = pd.read_csv(file_path_2)
    
    postgresDB.send_df_to_db(table_name='reviews', df=df_1)
    postgresDB.send_df_to_db(table_name='reviews', df=df_2)

def run_mongodb():
    mongodb = MongoDB(db_host=os.environ.get("MONGODB_HOST"),
                      db_port=os.environ.get("MONGODB_PORT"),
                      db_user=os.environ.get("MONGODB_USER"),
                      db_password=os.environ.get("MONGODB_PASSWORD"),
                      db_name=os.environ.get("MONGODB_DATABASE"))

    df_1 = pd.read_csv(file_path_1)
    df_2 = pd.read_csv(file_path_2)

    # Ustaw nazwę kolekcji w MongoDB
    collection_name = 'reviews'

    # Wysyłamy dane do MongoDB
    mongodb.send_df_to_db(collection_name=collection_name, df=df_1)
    mongodb.send_df_to_db(collection_name=collection_name, df=df_2)


if __name__ == '__main__':
    run_postgres()
    run_mongodb()