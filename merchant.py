import sqlite3
import logging

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database file path
DB_FILE = 'financials.db'

def create_merchant_table_if_not_exists():
    """Create the merchant table if it doesn't already exist, with an additional tx_category field."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS merchant (
        merchant_id TEXT PRIMARY KEY,
        city TEXT,
        region TEXT,
        country TEXT,
        phone_number TEXT,
        url TEXT,
        category TEXT,
        tx_category TEXT
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
    conn.close()

def get_unique_merchants_with_categories():
    """Fetch unique merchants with their categories from all_transactions view that are not already in the merchant table."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    select_query = """
        SELECT tx_merchant AS merchant_id, GROUP_CONCAT(DISTINCT tx_category) AS tx_categories
        FROM all_transactions 
        WHERE tx_merchant NOT IN (SELECT merchant_id FROM merchant)
        GROUP BY tx_merchant
    """
    
    cursor.execute(select_query)
    result = cursor.fetchall()
    conn.close()
    return {row[0]: row[1] for row in result}


def insert_merchants(merchants_with_categories):
    """Insert unique merchants into the merchant table with aggregated tx_category values."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    insert_query = """
    INSERT OR IGNORE INTO merchant (merchant_id, tx_category) VALUES (?, ?)
    """

    for merchant_id, tx_categories in merchants_with_categories.items():
        cursor.execute(insert_query, (merchant_id, tx_categories))
        if cursor.rowcount > 0:  # rowcount will be 1 if the row was inserted
            logging.info(f"Inserted merchant with ID {merchant_id} and categories {tx_categories}")
    
    conn.commit()
    conn.close()

def main():
    create_merchant_table_if_not_exists()
    
    merchants_with_categories = get_unique_merchants_with_categories()
    if not merchants_with_categories:
        return
    
    insert_merchants(merchants_with_categories)

if __name__ == "__main__":
    main()
