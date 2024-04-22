import logging
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, select, Table, update, delete, func
from sqlalchemy.orm import registry, sessionmaker


load_dotenv()


class PostgresDB:
    def __init__(self, db_host, db_port, db_name, db_user, db_password):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

        self.engine = create_engine(
            "postgresql://"
            + str(db_user)
            + ":"
            + str(db_password)
            + "@"
            + str(db_host)
            + ":"
            + str(db_port)
            + "/"
            + str(db_name)
        )
        self.mapper_registry = registry()
        self.Session = sessionmaker(bind=self.engine)

    def get_table(self, table_name: str):
        try:
            return Table(
                table_name, self.mapper_registry.metadata, autoload_with=self.engine
            )
        except Exception as e:
            logging.error(f"Could not get table: {str(e)}")
            raise

    def send_csv_to_db(self, table_name: str, csv_path: str):
        try:
            logging.info("Start sending CSV to db")
            df = pd.read_csv(csv_path, sep=";")
            df.to_sql(name=table_name, con=self.engine, if_exists="append", index=False)

            logging.info("Data has been written to database")
        except Exception as error:
            logging.error("Can not write data to database " + str(error))

    def get_table_sql(self, table_name: str):
        try:
            # logging.info(f"Fetching data from table '{table_name}'")
            query = f"SELECT * FROM {table_name};"
            conn = self.engine.connect()
            df = pd.read_sql(query, conn)
            # logging.info(f"Data fetched from table '{table_name}'")
            return df
        except Exception as error:
            logging.error(f"Error fetching data from table '{table_name}': {error}")

    def execute_transaction_with_data(self, table_name: str, df: pd.DataFrame):
        try:
            logging.info("Start executing transaction with data")
            session = self.Session()

            # Transaction starts
            logging.info("Start sending df to db")
            df.to_sql(name=table_name, con=self.engine, if_exists="append", index=False)
            logging.info("Data has been written to database")
            # Transaction ends

            session.commit()
            session.close()
            logging.info("Transaction with data executed successfully")
        except Exception as error:
            logging.error("Transaction with data failed: " + str(error))
            session.rollback()

    def update_students(self, data_path):
        df = pd.read_csv(data_path, sep=";")
        student_table = self.get_table("students")
        try:
            with self.engine.connect() as connection:
                for index, record in df.iterrows():
                    student_id = record["student_id"]
                    stmt = (
                        update(student_table)
                        .where(student_table.c.student_id == student_id)
                        .values(**record.to_dict())
                    )
                    connection.execute(stmt)
                    connection.commit()
            logging.info("Students table updated successfully.")
        except Exception as e:
            logging.error(f"Error updating students table: {e}")

    def update_courses(self, data_path):
        df = pd.read_csv(data_path, sep=";")
        course_table = self.get_table("courses")
        try:
            with self.engine.connect() as connection:
                for index, record in df.iterrows():
                    course_id = record["course_id"]
                    stmt = (
                        update(course_table)
                        .where(course_table.c.course_id == course_id)
                        .values(**record)
                    )
                    connection.execute(stmt)
                    connection.commit()
            logging.info("Courses table updated successfully.")
        except Exception as e:
            logging.error(f"Error updating courses table: {e}")

    def update_enrollments(self, data_path):
        df = pd.read_csv(data_path, sep=";")
        enrollment_table = self.get_table("enrollments")
        try:
            with self.engine.connect() as connection:
                for index, record in df.iterrows():
                    student_id = record["student_id"]
                    course_id = record["course_id"]
                    stmt = (
                        update(enrollment_table)
                        .where(
                            (enrollment_table.c.student_id == student_id)
                            & (enrollment_table.c.course_id == course_id)
                        )
                        .values(**record)
                    )
                    connection.execute(stmt)
                    connection.commit()
            logging.info("Enrollment table updated successfully.")
        except Exception as e:
            logging.error(f"Error updating enrollment table: {e}")

    def delete_table(self, table_name):
        try:
            with self.engine.connect() as connection:
                delete_stmt = delete(self.get_table(table_name)).where(True)
                connection.execute(delete_stmt)
                connection.commit()
            logging.info(f"All data cleared from {table_name} table successfully.")
        except Exception as e:
            logging.error(f"Error clearing data from table: {e}")

    def aggregate_students_by_program(self):
        try:
            student_table = self.get_table("students")
            session = self.Session()
            query = session.query(
                student_table.c.program_name,
                func.count().label("num_students")
            ).group_by(student_table.c.program_name)
            result = query.all()
            session.close()
            return result
        except Exception as e:
            logging.error(f"Error aggregating students by program: {e}")

    def join_students_courses(self):
        try:
            student_table = self.get_table("students")
            course_table = self.get_table("courses")
            enrollment_table = self.get_table("enrollments")

            session = self.Session()
            query = session.query(
                student_table.c.name,
                student_table.c.surname,
                course_table.c.course_name,
                course_table.c.instructor
            ).join(
                enrollment_table,
                student_table.c.student_id == enrollment_table.c.student_id
            ).join(
                course_table,
                course_table.c.course_id == enrollment_table.c.course_id
            )
            result = query.all()
            session.close()
            return result
        except Exception as e:
            logging.error(f"Error performing join query: {e}")