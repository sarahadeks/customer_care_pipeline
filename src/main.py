import os
from sqlalchemy import create_engine, Column, Integer, String, Boolean, TIMESTAMP, MetaData, Table
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from extract_process import extract_and_process_data
import logging
import json
import pandas as pd

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_tables():
    """Create tables in the PostgreSQL database."""
    try:
        db = create_engine(os.getenv('DB_CONN'))
        metadata = MetaData()

        raw_messages = Table(
            'customer_agent_messages', metadata,
            Column('senderDeviceType', String(50)),
            Column('customerId', Integer),
            Column('fromId', Integer),
            Column('toId', Integer),
            Column('chatStartedByMessage', Boolean),
            Column('orderId', Integer),
            Column('priority', Boolean),
            Column('resolution', Boolean),
            Column('agentId', Integer),
            Column('messageSentTime', TIMESTAMP),
            schema='raw_data'
        )

        raw_orders = Table(
            'orders', metadata,
            Column('orderId', Integer, primary_key=True),
            Column('cityCode', String(50)),
            schema='raw_data'
        )

        transformed_conversations = Table(
            'customer_agent_conversations', metadata,
            Column('order_id', Integer),
            Column('city_code', String(50)),
            Column('first_agent_message', TIMESTAMP),
            Column('first_customer_message', TIMESTAMP),
            Column('num_messages_agent', Integer),
            Column('num_messages_customer', Integer),
            Column('first_message_by', String(50)),
            Column('conversation_started_at', TIMESTAMP),
            Column('first_response_time_delay_seconds', Integer),
            Column('last_message_time', TIMESTAMP),
            Column('last_message_order_stage', String(50)),
            Column('resolved', Boolean),
            schema='transformed_data'
        )

        with db.connect() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS raw_data;")
            conn.execute("CREATE SCHEMA IF NOT EXISTS transformed_data;")

        metadata.create_all(db)
        logging.info("Schemas and tables created successfully.")
    except SQLAlchemyError as e:
        logging.error(f"Error creating tables: {e}")

def load_raw_data():
    """Load raw data into the PostgreSQL database."""
    try:
        db = create_engine(os.getenv('DB_CONN'))

        with open('/home/opeyemi/customer_care_data_pipeline/data/raw/customer_agent_messages.json') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        logging.info("Customer agent messages DataFrame created.")

        df.to_sql('customer_agent_messages', db, schema='raw_data', if_exists='replace', index=False)
        logging.info("Customer agent messages loaded into raw_data schema successfully.")

        with open('/home/opeyemi/customer_care_data_pipeline/data/raw/orders.json') as f:
            data2 = json.load(f)
        df2 = pd.DataFrame(data2)
        logging.info("Orders DataFrame created.")

        df2.to_sql('orders', db, schema='raw_data', if_exists='replace', index=False)
        logging.info("Orders loaded into raw_data schema successfully.")
    except Exception as e:
        logging.error(f"Error loading raw data: {e}")

def transform_data():
    """Transform data using a SQL script."""
    try:
        db = create_engine(os.getenv('DB_CONN'))
        sql_file_path = os.path.join(os.path.dirname(__file__), 'result.sql')

        with open(sql_file_path, 'r') as file:
            transform_query = file.read()

        with db.connect() as conn:
            conn.execute(transform_query)

        logging.info("Data transformation complete and loaded into transformed_data schema.")
    except SQLAlchemyError as e:
        logging.error(f"Error transforming data: {e}")

def load_transformed_data():
    """Load transformed data from PostgreSQL and save it as a CSV file."""
    try:
        db = create_engine(os.getenv('DB_CONN'))
        query = "SELECT * FROM transformed_data.customer_agent_conversations"
        df = pd.read_sql(query, db)
        output_file_path = '/home/opeyemi/customer_care_data_pipeline/data/transformed/customer_agent_conversations.csv'
        df.to_csv(output_file_path, index=False)
        logging.info(f"Data exported successfully to {output_file_path}")
    except SQLAlchemyError as e:
        logging.error(f"Error loading transformed data: {e}")

if __name__ == "__main__":
    create_tables()
    extract_and_process_data()
    load_raw_data()
    transform_data()
    load_transformed_data()
