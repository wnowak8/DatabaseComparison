import os

from dotenv import load_dotenv
from utils.logging import get_module_logging
import logging
import pandas as pd
from sqlalchemy import create_engine, select, Table, update
from sqlalchemy.orm import registry

load_dotenv()

logging.basicConfig(
    level=getattr(logging, "INFO"),
    format="%(asctime)s.%(msecs)03d-%(levelname)s-%(funcName)s()-%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class PostgresDB():
    def __init__(self, db_host, db_port, db_name, db_user, db_password):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

        self.engine = create_engine(
            'postgresql://' + str(db_user) + ':' + str(db_password) + '@' + str(db_host) + ':' + str(
                db_port) + '/' + str(db_name))
        self.mapper_registry = registry()

    def get_table(self, table_name: str):
        try:
            return Table(table_name,
                         self.mapper_registry.metadata,
                         autoload=True,
                         autoload_with=self.engine)
        except:
            logging.error("Could not get table")
            raise Exception

    def send_df_to_db(self, table_name: str, df: pd.DataFrame):
        try:
            logging.info("Start sending df to db")
            df.to_sql(name=table_name, con=self.engine, if_exists='append',
                      index=False)

            logging.info("Data has been written to database")
        except Exception as error:
            logging.error("Can not write data to database " + str(error))

    def get_df_from_sql(self, query: str):
        conn = self.engine.connect()
        return pd.read_sql(query, conn)

    def get_by_id(self, table_name: str, id: str):
        try:
            table = self.get_table(table_name=table_name)
            query = select(table).where(table.c.id == id)
            return self.get_df_from_sql(query=query)
        except Exception as ex:
            logging.exception(ex)

    def create(self, table_name: str, df: pd.DataFrame):
        df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)

    def update(self, table_name: str, id: str, data: dict):
        table = self.get_table(table_name=table_name)
        query = update(table).where(table.c.id == id).values(data)
        # print(query)
        with self.engine.connect() as conn:
            conn.execute(query)

    def delete(self, table_name: str, id: str, id_column):

        table = self.get_table(table_name=table_name)
        query = table.delete().where(id_column == id)
        with self.engine.connect() as conn:
            conn.execute(query)

    def get_last_row(self, table_name: str, column_name: str):
        table = self.get_table(table_name=table_name)
        query = table.select().order_by(table.c.column_name.desc()).limit(1)
        conn = self.engine.connect()
        return pd.read_sql(query, conn)