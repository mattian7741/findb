import sqlite3
import pandas as pd
import csv
import time
from datetime import datetime
import re
import argparse
import yaml
import hashlib

def load_config(config_file, account_name):
    """Load configuration from YAML for the specified account name."""
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    # Set defaults for parameters if they are not specified
    return config.get(account_name, {'header_row': 1, 'ignore_hash': [], 'required_column': [], 'headers': None, 'column_types': {}})

def detect_column_type(column_name, column_types):
    """Detects column type based on config or defaults to TEXT."""
    # Use specified column type from config or default to TEXT
    col_type = column_types.get(column_name, "TEXT")
    if col_type == "DATETIME":
        return 'INTEGER'  # Store dates as epoch seconds in INTEGER format
    elif col_type == "NUMERIC":
        return 'NUMERIC'
    return 'TEXT'  # Default to TEXT

def prepare_data(value, col_type):
    """Prepare data for insertion based on column type, handling datetime and numeric parsing."""
    if col_type == "DATETIME":
        # Parse as datetime and convert to UTC epoch seconds
        try:
            timestamp = pd.to_datetime(value, errors='raise')
            if not pd.isnull(timestamp):
                return int(timestamp.timestamp())
        except:
            return None  # Return None if unable to parse as DATETIME
    
    elif col_type == "NUMERIC":
        # Remove $, commas, and other characters, then convert to float
        try:
            value = re.sub(r'[^\d.-]', '', str(value))  # Remove non-numeric characters
            return float(value)
        except ValueError:
            return None  # Return None if unable to parse as NUMERIC
    
    # For TEXT and other types, return value as-is
    return value

def generate_unique_hash(row_values, table_name):
    """Generate a SHA-256 hash using row values and table name."""
    hash_input = table_name + ''.join(map(str, row_values))
    return hashlib.sha256(hash_input.encode()).hexdigest()

def clean_data(df, config):
    """Clean data based on configuration and rules."""
    headers = config.get('headers')
    
    if headers:
        # Apply custom headers if specified
        df.columns = headers
    else:
        # Otherwise, use specified header row and clean data as before
        header_row = config.get('header_row', 1) - 1
        df.columns = df.iloc[header_row]
        df = df.drop(index=list(range(header_row + 1))).reset_index(drop=True)
    
    df = df.loc[:, df.columns.notna()]  # Drop columns with missing headers
    return df

def check_duplicate_hash(cursor, table_name, unique_hash):
    """Check if a unique_hash already exists in the table."""
    cursor.execute(f"SELECT 1 FROM '{table_name}' WHERE unique_hash = ?", (unique_hash,))
    return cursor.fetchone() is not None

def insert_csv_to_db(account_name, csv_file, config_file='config.yaml', db_file='financials.db'):
    config = load_config(config_file, account_name)
    ignore_hashes = set(config.get('ignore_hash', []))  
    required_columns = config.get('required_column', [])  
    column_types = config.get('column_types', {})

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Load CSV file based on whether headers are specified
    headers = config.get('headers')
    if headers:
        df = pd.read_csv(csv_file, header=None)  # No header row in the CSV
        df.columns = headers  # Apply custom headers
    else:
        df = pd.read_csv(csv_file, header=None)
        df = clean_data(df, config)
    
    table_exists = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (account_name,)
    ).fetchone()
    
    if not table_exists:
        column_definitions = ['"unique_hash" TEXT', '"Tags" TEXT', '"created" INTEGER']
        for column in df.columns:
            # Use column_types directly without relying on data to determine type
            col_type = detect_column_type(column, column_types)
            column_definitions.append(f'"{column}" {col_type}')
        
        create_table_query = f'CREATE TABLE "{account_name}" ({", ".join(column_definitions)})'
        cursor.execute(create_table_query)
    
    for _, row in df.iterrows():
        row_values = [prepare_data(value, column_types.get(column, "TEXT")) for value, column in zip(row, df.columns)]
        unique_hash = generate_unique_hash(row_values, account_name)
        
        # Skip rows that match any hash in the ignore list
        if unique_hash in ignore_hashes:
            print(f"Skipping row with hash {unique_hash} as it's in the ignore list.")
            continue

        # Skip rows missing values in required columns
        missing_required = any(pd.isna(row[col]) or row[col] == "" for col in required_columns if col in df.columns)
        if missing_required:
            print(f"Skipping row with hash {unique_hash} due to missing required columns: {required_columns}")
            continue
        
        # Check for duplicates
        tags = "duplicate" if check_duplicate_hash(cursor, account_name, unique_hash) else ""

        # Capture the current timestamp in UTC epoch seconds for the 'created' column
        created_timestamp = int(time.time())

        # Insert the row with unique_hash, tags, created timestamp, and row values
        prepared_row = [unique_hash, tags, created_timestamp] + row_values
        placeholders = ', '.join(['?'] * len(prepared_row))
        insert_query = f'INSERT INTO "{account_name}" VALUES ({placeholders})'
        cursor.execute(insert_query, prepared_row)
    
    conn.commit()
    conn.close()
    print(f"Data from {csv_file} has been inserted into the table '{account_name}' with unique hashes and creation timestamps.")

def main():
    parser = argparse.ArgumentParser(description="Insert CSV data into an SQLite table.")
    parser.add_argument('account_name', type=str, help="The account name (table name) for the data.")
    parser.add_argument('csv_file', type=str, help="Path to the CSV file.")
    parser.add_argument('--config', type=str, default='config.yaml', help="Path to the YAML configuration file.")
    args = parser.parse_args()
    
    insert_csv_to_db(args.account_name, args.csv_file, args.config)

if __name__ == "__main__":
    main()
