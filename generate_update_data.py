import os
import csv
import random
import config

MONGO_DATA_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "mongo", "assets"
)
POSTGRES_DATA_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "postgres", "assets"
)


def generate_student_update_data(student_path):
    with open(
        os.path.join(POSTGRES_DATA_PATH, "students.csv"), mode="r", encoding="utf-8"
    ) as file:
        reader = csv.reader(file, delimiter=";")
        data = [next(reader)]  # Dodanie nagłówka

        for i, row in enumerate(reader, start=1):
            if i <= config.UPDATE_RECORDS:
                row[1] = f"Updated_Name_{i}"
                row[2] = f"Updated_Surname_{i}"
                row[3] = f"updated_student{i}@example.com"
                row[4] = (
                    f"+48 {random.randint(100, 999)} {random.randint(100, 999)} {random.randint(100, 999)}"
                )
                row[5] = f"Updated_Program_{i}"
                row[6] = random.randint(1, 10)
                row[7] = random.choice(["Full-time", "Part-time"])

                data.append(row)
            else:
                break  # Przerwanie pętli po dodaniu tysiąca wierszy

    with open(student_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, delimiter=";")
        writer.writerows(data)

    return data


def generate_course_update_data(courses_path):
    with open(
        os.path.join(POSTGRES_DATA_PATH, "courses.csv"), mode="r", encoding="utf-8"
    ) as file:
        reader = csv.reader(file, delimiter=";")
        data = [next(reader)]

        for i, row in enumerate(reader, start=1):
            if i <= config.UPDATE_RECORDS:
                row[1] = f"Updated_Course_{i}"
                row[2] = f"Updated_Instructor_{i}"
                row[3] = f"Updated_Room_{i}"
                row[4] = random.randint(1, 5)

                data.append(row)
            else:
                break 

    with open(courses_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, delimiter=";")
        writer.writerows(data)

    return data
