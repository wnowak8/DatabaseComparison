import os
from mongo.db.db_connector import MongoDB
from postgres.db.db_connector import PostgresDB


mongoDB = MongoDB(
    db_host=os.environ.get("MONGODB_HOST"),
    db_port=os.environ.get("MONGODB_PORT"),
    db_user=os.environ.get("MONGODB_USER"),
    db_password=os.environ.get("MONGODB_PASSWORD"),
    db_name=os.environ.get("MONGODB_DATABASE"),
)

postgresDB = PostgresDB(
    db_host=os.environ.get("POSTGRES_HOST"),
    db_port=os.environ.get("POSTGRES_PORT"),
    db_user=os.environ.get("POSTGRES_USER"),
    db_password=os.environ.get("POSTGRES_PASSWORD"),
    db_name=os.environ.get("POSTGRES_DATABASE"),
)

mongo_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "mongo", "data", "new_columns.json"
)
postgres_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "postgres", "data", "new_columns.csv"
)


postgresDB.send_csv_to_db("students", postgres_path)
mongoDB.send_json_to_db("students", mongo_path, "id")
