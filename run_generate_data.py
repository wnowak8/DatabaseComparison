import os
import config
from generate_data import (
    csv_to_json_with_courses,
    csv_to_json_with_students,
    generate_course_data,
    generate_student_data,
    get_enrollments_data,
    update_json_with_enrollments,
)


# generate_student_data(num_records=config.NUM_RECORDS)
# generate_course_data(num_records=config.NUM_RECORDS)

# file_path = os.path.join(config.POSTGRES_DATA_PATH, "enrollments.csv")

# get_enrollments_data(file_path=file_path)

students_json = csv_to_json_with_students(
    os.path.join(config.POSTGRES_DATA_PATH, "students.csv"),
    os.path.join(config.MONGO_DATA_PATH, "students.json"),
)

courses_json = csv_to_json_with_courses(
    os.path.join(config.POSTGRES_DATA_PATH, "courses.csv"),
    os.path.join(config.MONGO_DATA_PATH, "courses.json"),
)

update_json_with_enrollments(
    students_json,
    courses_json,
    os.path.join(config.POSTGRES_DATA_PATH, "enrollments.csv"),
)
