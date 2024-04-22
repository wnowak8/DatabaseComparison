from mongo.db.db_connector import MongoDB


student_data = {
    "student_id": "123e4567-e89b-12d3-a456-426614174000",
    "name": "John",
    "surname": "Doe",
    "email": "john.doe@example.com",
    "phone_number": "123456789",
    "program_name": "Computer Science",
    "semester": 2,
    "study_mode": "full-time",
}

db = MongoDB(
    db_host="localhost",
    db_port="27017",
    db_name="your_db_name",
    db_user="your_db_user",
    db_password="your_db_password",
)

db.execute_transaction_with_data(collection_name="students", data_json=student_data)
