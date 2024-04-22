import logging
import os
import config
from app.decorator import (
    average_performance_metrics,
    display,
    get_wmi_metrics,
    performance_metrics,
)

postgres_course_path = os.path.join(config.POSTGRES_DATA_PATH, "courses.csv")

postgres_student_path = os.path.join(config.POSTGRES_DATA_PATH, "students.csv")
postgres_enrollments_path = os.path.join(config.POSTGRES_DATA_PATH, "enrollments.csv")
postgres_update_course_path = os.path.join(
    config.POSTGRES_DATA_PATH, "update", "courses.csv"
)
postgres_update_student_path = os.path.join(
    config.POSTGRES_DATA_PATH, "update", "students.csv"
)
postgres_update_enrollments_path = os.path.join(
    config.POSTGRES_DATA_PATH, "update", "enrollments.csv"
)


@performance_metrics()
def create(postgresDB):
    logging.info("create")
    postgresDB.send_csv_to_db("students", postgres_student_path)
    postgresDB.send_csv_to_db("courses", postgres_course_path)
    postgresDB.send_csv_to_db("enrollments", postgres_enrollments_path)


@average_performance_metrics(total_connections=10)
def read_10(postgresDB):
    logging.info("read")
    students_df = postgresDB.get_table_sql("students")
    courses_df = postgresDB.get_table_sql("courses")
    enrollments_df = postgresDB.get_table_sql("enrollments")


@average_performance_metrics(total_connections=50)
def read_50(postgresDB):
    logging.info("read")
    students_df = postgresDB.get_table_sql("students")
    courses_df = postgresDB.get_table_sql("courses")
    enrollments_df = postgresDB.get_table_sql("enrollments")


@average_performance_metrics(total_connections=100)
def read_100(postgresDB):
    logging.info("read")
    students_df = postgresDB.get_table_sql("students")
    courses_df = postgresDB.get_table_sql("courses")
    enrollments_df = postgresDB.get_table_sql("enrollments")


@performance_metrics()
def update(postgresDB):
    logging.info("update")
    postgresDB.update_students(postgres_update_student_path)
    postgresDB.update_courses(postgres_update_course_path)
    postgresDB.update_enrollments(postgres_update_enrollments_path)
    m = get_wmi_metrics()
    display(m)


@performance_metrics()
def delete(postgresDB):
    logging.info("delete")
    postgresDB.delete_table("enrollments")
    postgresDB.delete_table("students")
    postgresDB.delete_table("courses")


@performance_metrics()
def aggregate(postgresDB):
    logging.info("aggregate")
    postgresDB.aggregate_students_by_program()


@performance_metrics()
def join(postgresDB):
    logging.info("join")
    postgresDB.join_students_courses()
