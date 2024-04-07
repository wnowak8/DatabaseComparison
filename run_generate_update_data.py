import os
import config
from generate_data import (
    csv_to_json_with_courses,
    csv_to_json_with_students,
    get_enrollments_data,
    update_json_with_enrollments,
)
from generate_update_data import (
    generate_course_update_data,
    generate_student_update_data,
)

generate_student_update_data(
    os.path.join(config.POSTGRES_DATA_PATH, "update", "students.csv")
)
generate_course_update_data(
    os.path.join(config.POSTGRES_DATA_PATH, "update", "courses.csv")
)

file_path = os.path.join(config.POSTGRES_DATA_PATH, "update", "enrollments.csv")
get_enrollments_data(file_path=file_path)


students_json = csv_to_json_with_students(
    os.path.join(config.POSTGRES_DATA_PATH, "update", "students.csv"),
    os.path.join(config.MONGO_DATA_PATH, "update", "students.json"),
)

courses_json = csv_to_json_with_courses(
    os.path.join(config.POSTGRES_DATA_PATH, "update", "courses.csv"),
    os.path.join(config.MONGO_DATA_PATH, "update", "courses.json"),
)

update_json_with_enrollments(students_json, courses_json, file_path)



