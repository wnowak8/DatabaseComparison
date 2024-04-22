import os
import config
import logging
from dotenv import load_dotenv
from mongo import crud as mongo_crud
from mongo.db.db_connector import MongoDB
from postgres import crud as postgres_crud
from postgres.db.db_connector import PostgresDB
from app.decorator import display, get_wmi_metrics

load_dotenv()


mongo_course_path = os.path.join(
    config.MONGO_DATA_PATH, "assets", "mongo_courses_updated.json"
)
mongo_student_path = os.path.join(
    config.MONGO_DATA_PATH, "assets", "mongo_students_updated.json"
)


postgres_course_path = os.path.join(
    config.POSTGRES_DATA_PATH, "assets", "courses.csv")
postgres_student_path = os.path.join(
    config.POSTGRES_DATA_PATH, "assets", "students.csv"
)
postgres_enrollment_path = os.path.join(
    config.POSTGRES_DATA_PATH, "assets", "enrollment.csv"
)


logging.basicConfig(
    level=getattr(logging, "INFO"),
    format="%(asctime)s.%(msecs)03d-%(levelname)s-%(funcName)s()-%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def run_postgres(postgresDB):
    postgreSQL = PostgresDB(
        db_host=os.environ.get("POSTGRES_HOST"),
        db_port=os.environ.get("POSTGRES_PORT"),
        db_user=os.environ.get("POSTGRES_USER"),
        db_password=os.environ.get("POSTGRES_PASSWORD"),
        db_name=os.environ.get("POSTGRES_DATABASE"),
    )

    postgresDB.send_csv_to_db("students", postgres_student_path)
    postgresDB.send_csv_to_db("courses", postgres_course_path)
    postgresDB.send_csv_to_db("enrollments", postgres_enrollment_path)
    postgres_crud.create(postgreSQL)
    postgres_crud.read_10(postgreSQL)
    postgres_crud.read_50(postgreSQL)
    postgres_crud.read_100(postgreSQL)
    postgres_crud.aggregate(postgreSQL)
    postgres_crud.join(postgreSQL)
    postgres_crud.update(postgreSQL)
    postgres_crud.delete(postgreSQL)


def run_mongodb(mongodb):
    mongodb = MongoDB(
        db_host=os.environ.get("MONGODB_HOST"),
        db_port=os.environ.get("MONGODB_PORT"),
        db_user=os.environ.get("MONGODB_USER"),
        db_password=os.environ.get("MONGODB_PASSWORD"),
        db_name=os.environ.get("MONGODB_DATABASE"),
    )

    mongodb.send_json_to_db(collection_name="courses",
                            json_file_path=mongo_course_path)
    mongodb.send_json_to_db(
        collection_name="students", json_file_path=mongo_student_path
    )
    mongo_crud.create(mongodb)
    mongo_crud.read_10(mongodb)
    mongo_crud.read_50(mongodb)
    mongo_crud.read_100(mongodb)
    mongo_crud.aggregate(mongodb)
    mongo_crud.join(mongodb)
    mongo_crud.update(mongodb)
    mongo_crud.delete(mongodb)


if __name__ == "__main__":
    a = get_wmi_metrics()
    display(a)
    run_postgres()
    logging.info("*" * 90)
    run_mongodb()
