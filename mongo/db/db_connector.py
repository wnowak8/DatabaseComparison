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

    def execute_transaction_with_data(self, collection_name: str, data_json: str):
        try:
            logging.info("Start executing transaction with data for MongoDB")
            with self.client.start_session() as session:
                with session.start_transaction():
                    data = json.loads(data_json)
                    self.db.insert_one(collection_name, data)
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
                if "course_id" in record or "student_id" in record:
                    record["_id"] = record.pop(id_name)

                filter_query = {"_id": record["_id"]}
                update_query = {"$set": record}

                requests.append(UpdateOne(filter_query, update_query))

            if requests:
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

    def aggregate_students_by_program(self):
        try:
            pipeline = [
                {"$group": {"_id": "$program_name", "num_students": {"$sum": 1}}},
                {"$project": {"_id": 0, "program_name": "$_id", "num_students": 1}},
            ]
            result = self.db.students.aggregate(pipeline)
            return list(result)
        except Exception as e:
            logging.error(f"Error aggregating students by program in MongoDB: {e}")

    def join_students_courses(self):
        try:
            pipeline = [
                {"$unwind": "$courses"},
                {
                    "$lookup": {
                        "from": "courses",
                        "localField": "courses",
                        "foreignField": "course_id",
                        "as": "course",
                    }
                },
                {"$unwind": "$course"},
                {
                    "$project": {
                        "_id": 0,
                        "student_name": {"$concat": ["$name", " ", "$surname"]},
                        "course_name": "$course.course_name",
                        "instructor": "$course.instructor",
                    }
                },
            ]
            result = self.db.students.aggregate(pipeline)
            return list(result)
        except Exception as e:
            logging.error(f"Error performing join query in MongoDB: {e}")
