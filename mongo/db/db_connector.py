import json
import logging
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()


class MongoDB:
    def __init__(self, db_host, db_port, db_name, db_user, db_password):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

        self.client = MongoClient(f"mongodb://{db_host}:{db_port}/{db_name}")
        self.db = self.client[db_name]

    def send_df_to_db(self, collection_name: str, df: pd.DataFrame):
        try:
            logging.info("Start sending df to MongoDB")
            records = df.to_dict(orient="records")
            self.db[collection_name].insert_many(records)
            logging.info("Data has been written to MongoDB")
        except Exception as error:
            logging.error("Can not write data to MongoDB: " + str(error))

    def send_json_to_db(self, collection_name: str, json_file_path: str, id_name: str):
        try:
            logging.info("Start sending JSON data to MongoDB")

            with open(json_file_path, "r") as json_file:
                data = json.load(json_file)
                # Iteracja przez każdy dokument w danych JSON
                for document in data:
                    # Pobieranie wartości "course_id" i ustawianie jej jako "_id" w dokumencie
                    id = document.pop(id_name)
                    document["_id"] = id
                # Wstawianie danych do kolekcji z poprawionymi dokumentami
                self.db[collection_name].insert_many(data)

            logging.info("Data from JSON file has been written to MongoDB")
        except Exception as error:
            logging.error("Can not write data to MongoDB: " + str(error))

    def get_collection(self, collection_name: str):
        try:
            collection = self.db[collection_name]
            return collection.find()
        except Exception as e:
            print(f"Error retrieving collection '{collection_name}': {e}")
            return None

    def get_by_id(self, collection_name: str, document_id: str):
        try:
            query = {"_id": document_id}
            return self.get_df_from_mongo(collection_name, query)
        except Exception as error:
            logging.error("Can not get data by ID from MongoDB: " + str(error))

    def get_last_document(self, collection_name: str, field_name: str):
        try:
            cursor = self.db[collection_name].find().sort(field_name, -1).limit(1)
            df = pd.DataFrame(list(cursor))
            return df
        except Exception as error:
            logging.error("Can not get last document from MongoDB: " + str(error))

    def execute_transaction_with_data(self, collection_name: str, df: pd.DataFrame):
        try:
            logging.info("Start executing transaction with data for MongoDB")
            with self.client.start_session() as session:
                with session.start_transaction():
                    records = df.to_dict(orient="records")
                    self.db[collection_name].insert_many(records)
            logging.info("Data has been written to MongoDB")
        except Exception as error:
            logging.error("Transaction with data failed for MongoDB: " + str(error))

    def update_documents(self, collection_name: str, json_file_path: str, id_name: str):
        try:
            logging.info("Starting update of documents in MongoDB from JSON file")

            with open(json_file_path, "r") as json_file:
                data = json.load(json_file)

            requests = []
            for record in data:
                # Sprawdź, czy pole "course_id" istnieje w dokumencie
                if "course_id" in record or "student_id" in record:
                    # Przypisz wartość pola "course_id" do pola "_id"
                    record["_id"] = record.pop(id_name)

                # Stwórz zapytanie aktualizacji
                filter_query = {"_id": record["_id"]}
                update_query = {"$set": record}

                # Dodaj zapytanie do listy
                requests.append(UpdateOne(filter_query, update_query))

            if requests:
                # Wykonaj masową aktualizację
                result = self.db[collection_name].bulk_write(requests)
                logging.info(f"Updated {result.modified_count} documents in MongoDB")
            else:
                logging.warning("No documents to update")
        except Exception as error:
            logging.error(
                "Can not update documents in MongoDB from JSON file: " + str(error)
            )

    def delete_collection(self, collection_name: str):
        try:
            logging.info(
                f"Starting deletion of all documents in collection '{collection_name}'"
            )
            result = self.db[collection_name].delete_many({})
            logging.info(
                f"Deleted {result.deleted_count} documents from collection '{collection_name}'"
            )
        except Exception as error:
            logging.error(
                f"Can not delete documents from collection '{collection_name}': {error}"
            )
