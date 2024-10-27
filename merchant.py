import sqlite3

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
    """Retrieve unique merchants with associated tx_categories from all_transactions view."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    query = """
    SELECT tx_merchant, tx_category
    FROM all_transactions
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    merchants_with_categories = {}

    for merchant, category in rows:
        if merchant not in merchants_with_categories:
            merchants_with_categories[merchant] = set()
        # Only add category if it's not None
        if category is not None:
            merchants_with_categories[merchant].add(category)

    # Convert each merchant's category set to a comma-delimited string
    for merchant in merchants_with_categories:
        merchants_with_categories[merchant] = ",".join(sorted(merchants_with_categories[merchant]))

    conn.close()
    
    return merchants_with_categories

def insert_merchants(merchants_with_categories):
    """Insert unique merchants into the merchant table with aggregated tx_category values."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    insert_query = """
    INSERT OR REPLACE INTO merchant (merchant_id, tx_category) VALUES (?, ?)
    """
    
    for merchant_id, tx_categories in merchants_with_categories.items():
        cursor.execute(insert_query, (merchant_id, tx_categories))
    
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
