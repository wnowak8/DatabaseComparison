import csv
import json
import os
import random
import uuid
import config


def generate_student_data(num_records):
    data = [
        [
            "student_id",
            "name",
            "surname",
            "email",
            "phone_number",
            "program_name",
            "semester",
            "study_mode",
        ]
    ]

    for i in range(1, num_records + 1):
        student_id = str(uuid.uuid4())
        name = f"Name_{i}"
        surname = f"Surname_{i}"
        email = f"student{i}@example.com"
        phone_number = f"+48 {random.randint(100, 999)} {random.randint(100, 999)} {random.randint(100, 999)}"
        program_name = f"Program_{i}"
        semester = random.randint(1, 10)
        study_mode = random.choice(["Full-time", "Part-time"])

        data.append(
            [
                student_id,
                name,
                surname,
                email,
                phone_number,
                program_name,
                semester,
                study_mode,
            ]
        )

    file_path = os.path.join(config.POSTGRES_DATA_PATH, "students.csv")
    with open(file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, delimiter=";")
        writer.writerows(data)
    return data


def generate_course_data(num_records):
    data = [["course_id", "course_name", "instructor", "room", "credits"]]

    for i in range(1, num_records + 1):
        course_id = str(uuid.uuid4())
        course_name = f"Course_{i}"
        instructor = f"Instructor_{i}"
        room = f"Room_{i}"
        credits = random.randint(1, 5)

        data.append([course_id, course_name, instructor, room, credits])

    file_path = os.path.join(config.POSTGRES_DATA_PATH, "courses.csv")

    with open(file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, delimiter=";")
        writer.writerows(data)

    return data


def generate_enrollments_data(student_data, course_data, file_path):
    data = [["enrollment_id", "student_id", "course_id"]]

    for student_row in student_data[1:]:  # Pomijamy nagłówek
        student_id = student_row[0]
        for course_row in random.sample(course_data[1:], random.randint(1, 4)):
            course_id = course_row[0]
            enrollment_id = str(uuid.uuid4())
            data.append([enrollment_id, student_id, course_id])

    with open(file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, delimiter=";")
        writer.writerows(data)


def get_enrollments_data(file_path):

    # Wczytaj dane dla studentów
    with open(
        os.path.join(config.POSTGRES_DATA_PATH, "students.csv"),
        mode="r",
        encoding="utf-8",
    ) as file:
        reader = csv.reader(file, delimiter=";")
        student_data = list(reader)

    # Wczytaj dane dla kursów
    with open(
        os.path.join(config.POSTGRES_DATA_PATH, "courses.csv"),
        mode="r",
        encoding="utf-8",
    ) as file:
        reader = csv.reader(file, delimiter=";")
        course_data = list(reader)

    generate_enrollments_data(student_data, course_data, file_path)


def csv_to_json_with_courses(csv_filename, json_filename):
    data = []

    with open(csv_filename, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter=";")
        for row in reader:
            row["students"] = []
            data.append(row)

    with open(json_filename, mode="w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    return data


def csv_to_json_with_students(csv_filename, json_filename):
    data = []

    with open(csv_filename, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter=";")
        for row in reader:
            row["courses"] = []
            data.append(row)

    with open(json_filename, mode="w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    return data


# def update_json_with_enrollments(student_json, course_json, enrollments_csv):
#     with open(enrollments_csv, mode="r", encoding="utf-8") as file:
#         reader = csv.DictReader(file, delimiter=";")
#         for row in reader:
#             student_id = row["student_id"]
#             course_id = row["course_id"]

#             # Dodaj course_id do pola courses danego studenta
#             for student in student_json:
#                 if student["student_id"] == student_id:
#                     student["courses"].append(course_id)

#             # Dodaj student_id do pola students danego kursu
#             for course in course_json:
#                 if course["course_id"] == course_id:
#                     course["students"].append(student_id)

#     file_path_students = os.path.join(config.MONGO_DATA_PATH, "students.json")
#     file_path_courses = os.path.join(config.MONGO_DATA_PATH, "courses.json")

#     with open(file_path_students, mode="w", encoding="utf-8") as file:
#         json.dump(student_json, file, indent=2)

#     with open(file_path_courses, mode="w", encoding="utf-8") as file:
#         json.dump(course_json, file, indent=2)


def update_student_json(student_json, enrollments_csv):
    with open(enrollments_csv, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter=";")
        for row in reader:
            student_id = row["student_id"]
            course_id = row["course_id"]

            # Dodaj course_id do pola courses danego studenta
            for student in student_json:
                if student["student_id"] == student_id:
                    student["courses"].append(course_id)

    return student_json


def update_course_json(course_json, enrollments_csv):
    with open(enrollments_csv, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter=";")
        for row in reader:
            student_id = row["student_id"]
            course_id = row["course_id"]

            # Dodaj student_id do pola students danego kursu
            for course in course_json:
                if course["course_id"] == course_id:
                    course["students"].append(student_id)

    return course_json


def save_json_to_file(data, file_path):
    with open(file_path, mode="w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)


def save_json_to_file(data, file_path):
    with open(file_path, mode="w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)


import concurrent.futures


def update_json_with_enrollments(student_json, course_json, enrollments_csv):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_student_json = executor.submit(
            update_student_json, student_json, enrollments_csv
        )
        future_course_json = executor.submit(
            update_course_json, course_json, enrollments_csv
        )

    updated_student_json = future_student_json.result()
    updated_course_json = future_course_json.result()

    file_path_students = os.path.join(config.MONGO_DATA_PATH, "students.json")
    file_path_courses = os.path.join(config.MONGO_DATA_PATH, "courses.json")

    save_json_to_file(updated_student_json, file_path_students)
    save_json_to_file(updated_course_json, file_path_courses)
