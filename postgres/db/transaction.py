# Dane studenta
import pandas as pd
from postgres.db.db_connector import PostgresDB


student_data = {
    "student_id": "123e4567-e89b-12d3-a456-426614174000",
    "name": "John",
    "surname": "Doe",
    "email": "john.doe@example.com",
    "phone_number": "123456789",
    "program_name": "Computer Science",
    "semester": 2,
    "study_mode": "full-time"
}

# Inicjalizacja połączenia z bazą danych
db = PostgresDB(
    db_host='localhost',
    db_port='5432',
    db_name='your_db_name',
    db_user='your_db_user',
    db_password='your_db_password'
)

# Konwersja słownika z danymi studenta do obiektu DataFrame
student_df = pd.DataFrame([student_data])

# Wysłanie danych studenta do tabeli students w bazie danych za pomocą transakcji
db.execute_transaction_with_data(table_name='students', df=student_df)