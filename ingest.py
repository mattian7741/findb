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
    return config.get(account_name, {
        'header_row': 1,
        'ignore_hash': [],
        'required_column': [],
        'headers': None,
        'column_types': {},
        'hash_columns': []
    })

def detect_column_type(column_name, column_types):
    """Detects column type based on config or defaults to TEXT."""
    col_type = column_types.get(column_name, "TEXT")
    if col_type == "DATETIME":
        return 'INTEGER'  # Store dates as epoch seconds in INTEGER format
    elif col_type == "NUMERIC":
        return 'NUMERIC'
    return 'TEXT'  # Default to TEXT

def prepare_data(value, col_type):
    """Prepare data for insertion based on column type, handling datetime and numeric parsing."""
    if col_type == "DATETIME":
        try:
            timestamp = pd.to_datetime(value, errors='raise')
            # Determine UTC offset based on month (rough guide for EDT and EST)
            if timestamp.month in [4, 5, 6, 7, 8, 9, 10]:  # Typically EDT months
                offset_hours = 4
            else:
                offset_hours = 5
            return int((timestamp + pd.Timedelta(hours=offset_hours)).timestamp()) if not pd.isnull(timestamp) else None
        except:
            return None
    elif col_type == "NUMERIC":
        try:
            value = re.sub(r'[^\d.-]', '', str(value))
            return float(value)
        except ValueError:
            return None
    return value  # For TEXT and other types

def generate_unique_hash(row_values, table_name):
    """Generate a SHA-256 hash using row values and table name."""
    hash_input = table_name + ''.join(map(str, row_values))
    return hashlib.sha256(hash_input.encode()).hexdigest()


def generate_unique_hash_y(row_values, table_name, hash_columns):
    """Generate a SHA-256 hash using specified row values and hash columns."""
    if hash_columns:
        # Filter values based on hash_columns if provided
        hash_input = table_name + ''.join(str(row_values[col]) for col in hash_columns if col in row_values)
    else:
        # Concatenate all values into a single string if no specific hash_columns provided
        hash_input = table_name + ''.join(str(value) for value in row_values.values())
    return hashlib.sha256(hash_input.encode()).hexdigest()

def generate_unique_hash_x(row_values, table_name, hash_columns):
    """Generate a SHA-256 hash using specified row values and hash columns."""
    hash_input = ""
    if False: #hash_columns:
        # Filter values based on hash_columns if provided
        hash_input = table_name + ''.join(str(row_values[col]) for col in hash_columns if col in row_values)
    else:
        # Concatenate all values into a single string if no specific hash_columns provided
        hash_input = table_name + ''.join(str(value) for value in row_values.values())
    hashed = hashlib.sha256(hash_input.encode()).hexdigest()
    print(hashed)
    return hashed

def clean_data(df, config):
    """Clean data based on configuration and rules."""
    headers = config.get('headers')
    if headers:
        df.columns = headers
    else:
        header_row = config.get('header_row', 1) - 1
        df.columns = df.iloc[header_row]
        df = df.drop(index=list(range(header_row + 1))).reset_index(drop=True)
    df = df.loc[:, df.columns.notna()]
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
    hash_columns = config.get('hash_columns', [])

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

    # Check if table exists and create if not
    table_exists = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (account_name,)
    ).fetchone()

    if not table_exists:
        # Define default columns with their types
        column_definitions = [
            '"unique_hash" TEXT',
            '"Tags" TEXT',
            '"created" INTEGER'
        ]
        # Append additional columns from CSV based on configuration or detect type dynamically
        for column in df.columns:
            col_type = detect_column_type(column, column_types)
            column_definitions.append(f'"{column}" {col_type}')

        create_table_query = f'CREATE TABLE "{account_name}" ({", ".join(column_definitions)})'
        cursor.execute(create_table_query)

    for _, row in df.iterrows():
        row_values = {column: prepare_data(row[column], column_types.get(column, "TEXT")) for column in df.columns}
        if hash_columns:
            hash_values = [row_values[col] for col in hash_columns if col in row_values]
        else:
            hash_values = list(row_values.values())
        unique_hash = generate_unique_hash(hash_values, account_name)
        # Skip rows missing values in required columns
        missing_required = any(pd.isna(row[col]) or row[col] == "" for col in required_columns if col in df.columns)
        if missing_required:
            print(f"Skipping row with hash {unique_hash} due to missing required columns: {required_columns}")
            continue
        # if unique_hash in ignore_hashes or any(row_values.get(col) in [None, ""] for col in required_columns):
        #      continue

        # Check for duplicates
        tags = "duplicate" if check_duplicate_hash(cursor, account_name, unique_hash) else ""
        # Capture the current timestamp in UTC epoch seconds for the 'created' column
        # created_timestamp = int(time.time())
        # if check_duplicate_hash(cursor, account_name, unique_hash):
        #     continue

        # Prepare row for insertion
        # tags = ""  # Default value for Tags
        created_timestamp = int(time.time())  # Current timestamp in epoch seconds
        insert_values = [unique_hash, tags, created_timestamp] + [row_values[col] for col in df.columns]
        placeholders = ', '.join(['?'] * len(insert_values))
        insert_query = f'INSERT INTO "{account_name}" VALUES ({placeholders})'
        cursor.execute(insert_query, insert_values)
    
    conn.commit()
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Insert CSV data into an SQLite table.")
    parser.add_argument('account_name', type=str, help="The account name for the data.")
    parser.add_argument('csv_file', type=str, help="Path to the CSV file.")
    parser.add_argument('--config', type=str, default='config.yaml', help="Path to the YAML configuration file.")
    args = parser.parse_args()
    insert_csv_to_db(args.account_name, args.csv_file, args.config)

if __name__ == "__main__":
    main()
