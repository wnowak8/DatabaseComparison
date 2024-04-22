import os
import config
import logging
from app.decorator import average_performance_metrics, performance_metrics

mongo_course_path = os.path.join(config.MONGO_DATA_PATH, "courses.json")
mongo_student_path = os.path.join(config.MONGO_DATA_PATH, "students.json")
mongo_update_course_path = os.path.join(
    config.MONGO_DATA_PATH, "update", "courses.json"
)
mongo_update_student_path = os.path.join(
    config.MONGO_DATA_PATH, "update", "students.json"
)


@performance_metrics()
def create(mongoDB):
    logging.info("create")
    mongoDB.send_json_to_db("students", mongo_student_path, "student_id")
    mongoDB.send_json_to_db("courses", mongo_course_path, "course_id")


@average_performance_metrics(total_connections=10)
def read_10(mongoDB):
    logging.info("read")
    students = mongoDB.get_collection("students")
    courses = mongoDB.get_collection("courses")


@average_performance_metrics(total_connections=50)
def read_50(mongoDB):
    logging.info("read")
    students = mongoDB.get_collection("students")
    courses = mongoDB.get_collection("courses")


@average_performance_metrics(total_connections=100)
def read_100(mongoDB):
    logging.info("read")
    students = mongoDB.get_collection("students")
    courses = mongoDB.get_collection("courses")


@performance_metrics()
def update(mongoDB):
    logging.info("update")
    mongoDB.update_documents("students", mongo_update_student_path, "student_id")
    mongoDB.update_documents("courses", mongo_update_course_path, "course_id")


@performance_metrics()
def delete(mongoDB):
    logging.info("delete")
    mongoDB.delete_collection("students")
    mongoDB.delete_collection("courses")


@performance_metrics()
def aggregate(mongoDB):
    logging.info("aggregate")
    mongoDB.aggregate_students_by_program()


@performance_metrics()
def join(mongoDB):
    logging.info("join")
    mongoDB.join_students_courses()
