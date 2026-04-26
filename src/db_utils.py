from sqlalchemy import create_engine, inspect, MetaData, Table, Column, Integer, String, Text, TIMESTAMP, text
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import logging
from src.secrets_manager import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

log = logging.getLogger(__name__)

class DBLoader:

    def __init__(self, table_name, engine=None):
        try:
            self.__table_name = table_name
            self.__engine = engine or self.__create_engine()
            
            # Define table schema for SQLAlchemy Core operations
            metadata = MetaData()
            self.__table = Table(
                self.__table_name, metadata,
                Column('num', Integer, primary_key=True),
                Column('title', String(255)),
                Column('safe_title', String(255)),
                Column('img_url', Text),
                Column('alt_text', Text),
                Column('transcript', Text),
                Column('year', Integer),
                Column('month', Integer),
                Column('day', Integer),
                Column('inserted_at', TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
            )
            
            log.info("DBLoader initialized with table: %s", self.__table_name)
        
        except Exception as e:
            log.error("Not able to initialise DB - %s", e)
            raise

    def __create_engine(self):
        db_host = DB_HOST
        db_port = DB_PORT
        db_name = DB_NAME
        db_user = DB_USER
        db_password = DB_PASSWORD

        if not all([db_host, db_name, db_user, db_password, db_port]):
            raise ValueError("Database connection parameters are not set.")
        try:
            engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
            log.info("Database engine created successfully.")
            return engine
        except Exception as e:
            log.error("Failed to create database engine. Error: %s", e)
            raise e
        
    def __ensure_table_exists(self):
        """Create table if it doesn't exist using SQLAlchemy Core."""

        insp = inspect(self.__engine)
        if not insp.has_table(self.__table_name):
            self.__table.create(self.__engine)
            log.info("Created table: %s", self.__table_name)
    
    def insert_data(self, data):
        """Insert data into the database."""

        try:
            if data.empty:
                log.info("Nothing to upsert - Data is empty!!!")
                return 0
            
            # Rename columns to match DB schema
            data = data.rename(columns={"alt": "alt_text", "img": "img_url"})
            
            # Keep only columns that exist in the table
            db_cols = ["num", "title", "safe_title", "img_url", "alt_text", 
                    "transcript", "year", "month", "day"]
            records = data[db_cols].where(data[db_cols].notna(), None).to_dict(orient="records")
            
            self.__ensure_table_exists()
            
            stmt = insert(self.__table).values(records)
            stmt = stmt.on_conflict_do_nothing(index_elements=["num"])
            
            with self.__engine.begin() as conn:
                result = conn.execute(stmt)
                inserted = result.rowcount
            
            log.info("Inserted %s new records into %s", inserted, self.__table_name)
            return inserted
        except Exception as e:
            log.error("Failed to insert data into %s - %s", self.__table_name, e)
            return 0