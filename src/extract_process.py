import json
import os
import re
import pandas as pd
from dateutil import parser
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime, timezone
import logging

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_timestamp(timestamp_str):
    try:
        # Ensure the time component is at least HH:MM:SS
        date_part, time_part = timestamp_str.split('T')
        time_part = time_part.rstrip('Z')
        
        # Split the time part into components
        time_components = time_part.split(':')
        
        # Normalize hours
        if len(time_components[0]) == 1:
            time_components[0] = '0' + time_components[0]
        
        # Normalize minutes
        if len(time_components[1]) == 1:
            time_components[1] = '0' + time_components[1]
        
        # Normalize seconds by ensuring it has exactly two digits
        if len(time_components[2]) > 2:
            time_components[2] = time_components[2][:2]
        
        # Reconstruct the normalized time part
        normalized_time_part = ':'.join(time_components) + 'Z'
        
        # Reconstruct the normalized timestamp
        normalized_timestamp = f"{date_part}T{normalized_time_part}"
        
        # Parse the normalized timestamp
        parsed_datetime = parser.isoparse(normalized_timestamp)
        
        # Convert to datetime with UTC timezone
        parsed_datetime = parsed_datetime.replace(tzinfo=timezone.utc)
        
        return parsed_datetime
    except Exception as e:
        # Handle any parsing errors
        print(f"Error parsing timestamp: {e}")
        return None

def extract_and_process_data():
    """Extract and process raw data, and load it to the database and processed folder."""
    try:
        customer_agent_messages_path = '/mnt/c/Data Engineer Technical Test/input_data/customer_agent_messages.json'
        orders_path = '/mnt/c/Data Engineer Technical Test/input_data/orders.json'
        dir_path = os.path.dirname(os.path.realpath(__file__))
        raw_data_cm = os.path.join(dir_path, 'data/raw')
        os.makedirs(raw_data_cm, exist_ok=True)

        destination_customer_agent_messages = os.path.join(raw_data_cm, 'customer_agent_messages.json')
        destination_orders = os.path.join(raw_data_cm, 'orders.json')

        with open(customer_agent_messages_path, 'r') as file:
            customer_agent_messages_data = json.load(file)
        with open(orders_path, 'r') as file:
            orders_data = json.load(file)

        with open(destination_customer_agent_messages, 'w') as file:
            json.dump(customer_agent_messages_data, file, indent=4)
        with open(destination_orders, 'w') as file:
            json.dump(orders_data, file, indent=4)

        logging.info(f"Data successfully copied to {destination_customer_agent_messages} and {destination_orders}")

        customer_agent_messages_df = pd.DataFrame(customer_agent_messages_data)
        orders_df = pd.DataFrame(orders_data)

        customer_agent_messages_df['messageSentTime'] = customer_agent_messages_df['messageSentTime'].apply(parse_timestamp)
        customer_agent_messages_df['priority'] = customer_agent_messages_df['priority'].fillna(False).astype(bool)

        logging.info("DataFrames created and processed.")

        db_url = os.getenv('DB_CONN')
        engine = create_engine(db_url)

        destination_dir_processed = os.path.join(dir_path, 'data/processed')
        os.makedirs(destination_dir_processed, exist_ok=True)

        customer_agent_messages_df.to_sql('customer_agent_messages', con=engine, schema='processed_data', if_exists='replace', index=False)
        orders_df.to_sql('orders', con=engine, schema='processed_data', if_exists='replace', index=False)

        customer_agent_messages_df.to_csv(os.path.join(destination_dir_processed, 'customer_agent_messages.csv'), index=False)
        orders_df.to_csv(os.path.join(destination_dir_processed, 'orders.csv'), index=False)

        logging.info("Data saved to processed folder and database.")
    except Exception as e:
        logging.error(f"Error in extract_and_process_data: {e}")
