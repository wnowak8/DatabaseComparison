import os
from dotenv import load_dotenv
import logging
from pymongo import MongoClient
import pandas as pd

load_dotenv()


class MongoDB():
    def __init__(self, db_host, db_port, db_name, db_user, db_password):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

        self.client = MongoClient(f"mongodb://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
        self.db = self.client[db_name]

    def send_df_to_db(self, collection_name: str, df: pd.DataFrame):
        try:
            logging.info("Start sending df to MongoDB")
            records = df.to_dict(orient='records')
            self.db[collection_name].insert_many(records)
            logging.info("Data has been written to MongoDB")
        except Exception as error:
            logging.error("Can not write data to MongoDB: " + str(error))

    def get_df_from_mongo(self, collection_name: str, query: dict = None):
        try:
            cursor = self.db[collection_name].find(query) if query else self.db[collection_name].find()
            df = pd.DataFrame(list(cursor))
            return df
        except Exception as error:
            logging.error("Can not read data from MongoDB: " + str(error))

    def get_by_id(self, collection_name: str, document_id: str):
        try:
            query = {"_id": document_id}
            return self.get_df_from_mongo(collection_name, query)
        except Exception as error:
            logging.error("Can not get data by ID from MongoDB: " + str(error))

    def update(self, collection_name: str, document_id: str, data: dict):
        try:
            query = {"_id": document_id}
            update_query = {"$set": data}
            self.db[collection_name].update_one(query, update_query)
        except Exception as error:
            logging.error("Can not update data in MongoDB: " + str(error))

    def delete(self, collection_name: str, document_id: str):
        try:
            query = {"_id": document_id}
            self.db[collection_name].delete_one(query)
        except Exception as error:
            logging.error("Can not delete data from MongoDB: " + str(error))

    def get_last_document(self, collection_name: str, field_name: str):
        try:
            cursor = self.db[collection_name].find().sort(field_name, -1).limit(1)
            df = pd.DataFrame(list(cursor))
            return df
        except Exception as error:
            logging.error("Can not get last document from MongoDB: " + str(error))
