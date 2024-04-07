import os
from app.decorator import display, get_wmi_metrics
import config
import logging
from mongo.db.db_connector import MongoDB
from postgres import crud as postgres_crud
from mongo import crud as mongo_crud
from postgres.db.db_connector import PostgresDB
from dotenv import load_dotenv

# Wczytanie zmiennych środowiskowych z pliku .env
load_dotenv()

PATH_TO_MONGODB = os.path.join(os.getcwd(), "mongo")
PATH_TO_POSTGRESQL = os.path.join(os.getcwd(), "postgres")
mongo_course_path = os.path.join(
    PATH_TO_MONGODB, "assets", "mongo_courses_updated.json"
)
mongo_student_path = os.path.join(
    PATH_TO_MONGODB, "assets", "mongo_students_updated.json"
)


postgres_course_path = os.path.join(PATH_TO_POSTGRESQL, "assets", "courses.csv")
postgres_student_path = os.path.join(PATH_TO_POSTGRESQL, "assets", "students.csv")
postgres_enrollment_path = os.path.join(PATH_TO_POSTGRESQL, "assets", "enrollment.csv")


logging.basicConfig(
    level=getattr(logging, "INFO"),
    format="%(asctime)s.%(msecs)03d-%(levelname)s-%(funcName)s()-%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# def dateparse (time_in_secs):
#     return datetime.fromtimestamp(float(time_in_secs))

# def send_chunks_to_database(database, table_collection_name, file_path):
#     chunk_size = 1000  # Możesz dostosować rozmiar chunka do swoich potrzeb
#     reader = pd.read_csv(file_path,
#                          chunksize=chunk_size,
#                          decimal='.',
#                          parse_dates=['date'],
#                          date_format='%Y-%m-%d %H:%M:%S.%f',
#                          index_col='date')

#     for i, chunk in enumerate(reader):
#         chunk = chunk.replace({',': '.'}, regex=True)
#         database.send_df_to_db(table_collection_name, chunk)
#         logging.info(f"Processed chunk {i+1}")


def run_postgres(postgresDB):
    # postgresDB = PostgresDB(db_host=os.environ.get("POSTGRES_HOST"),
    #                         db_port=os.environ.get("POSTGRES_PORT"),
    #                         db_user=os.environ.get("POSTGRES_USER"),
    #                         db_password=os.environ.get("POSTGRES_PASSWORD"),
    #                         db_name=os.environ.get("POSTGRES_DATABASE"))

    # postgresDB.send_csv_to_db('students', postgres_student_path)
    # postgresDB.send_csv_to_db('courses', postgres_course_path)
    # postgresDB.send_csv_to_db('enrollments', postgres_enrollment_path)
    postgres_crud.create(postgresDB)
    postgres_crud.read(postgresDB)
    postgres_crud.update(postgresDB)
    postgres_crud.delete(postgresDB)


def run_mongodb(mongodb):
    # mongodb = MongoDB(
    #     db_host=os.environ.get("MONGODB_HOST"),
    #     db_port=os.environ.get("MONGODB_PORT"),
    #     db_user=os.environ.get("MONGODB_USER"),
    #     db_password=os.environ.get("MONGODB_PASSWORD"),
    #     db_name=os.environ.get("MONGODB_DATABASE"),
    # )

    # mongodb.send_json_to_db(collection_name="courses", json_file_path=mongo_course_path)
    # mongodb.send_json_to_db(
    #     collection_name="students", json_file_path=mongo_student_path
    # )
    mongo_crud.create(mongodb)
    mongo_crud.read(mongodb)
    mongo_crud.update(mongodb)
    mongo_crud.delete(mongodb)


if __name__ == "__main__":
    # a = get_wmi_metrics()
    # display(a)
    mongodb = MongoDB(
        db_host=os.environ.get("MONGODB_HOST"),
        db_port=os.environ.get("MONGODB_PORT"),
        db_user=os.environ.get("MONGODB_USER"),
        db_password=os.environ.get("MONGODB_PASSWORD"),
        db_name=os.environ.get("MONGODB_DATABASE"),
    )

    # Tworzenie instancji klasy PostgresDB
    # postgresDB = PostgresDB(
    #     db_host=os.environ.get("POSTGRES_HOST"),
    #     db_port=os.environ.get("POSTGRES_PORT"),
    #     db_user=os.environ.get("POSTGRES_USER"),
    #     db_password=os.environ.get("POSTGRES_PASSWORD"),
    #     db_name=os.environ.get("POSTGRES_DATABASE"),
    # )

    # Wywołanie metody get_table() na instancji klasy PostgresDB
    # a = postgresDB.get_table(table_name="students")
    # postgres_update_course_path = os.path.join(
    #     config.POSTGRES_DATA_PATH, "update", "courses.csv"
    # )
    # postgres_update_student_path = os.path.join(
    #     config.POSTGRES_DATA_PATH, "update", "students.csv"
    # )
    # postgres_update_enrollments_path = os.path.join(
    #     config.POSTGRES_DATA_PATH, "update", "enrollments.csv"
    # )

    # run_postgres()
    # metrics = get_windows_performance_metrics()
    # get_initial_performance_metrics()
    m = get_wmi_metrics()
    display(m)
    logging.info("*" * 90)
    # mongo_crud.create(mongodb)
    # mongo_crud.read(mongodb)
    # mongo_crud.update(mongodb)
    # mongo_crud.delete(mongodb)
    # postgres_crud.update(postgresDB)
    # # run_mongodb(mongodb)

    # # run_postgres(postgresDB)
    # # get_performance_metrics()
    # logging.info("*" * 90)
    # get_windows_performance_metrics()
    # logging.info("*" * 90)
    # get_performance_metrics()
    # logging.info("*" * 90)
    # get_windows_performance_metrics()
    # get_all_data_postgres_with_performance_metrics(table_name='students', postgresDB=postgresDB, total_connections=5)
    # get_all_data_mongodb_with_performance_metrics(collection_name='students', mongodb=mongodb, total_connections=5)
