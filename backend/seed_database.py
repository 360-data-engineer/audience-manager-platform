# backend/seed_database.py
import sqlite3
import random
from faker import Faker
from datetime import datetime, timedelta
import uuid

# --- Configuration ---
DATABASE_PATH = 'db/audience_manager.db'
NUM_USERS = 100
TRANSACTIONS_PER_USER = 50
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 12, 31)

CATEGORIES = ['Electronics', 'Groceries', 'Apparel', 'Dining', 'Travel', 'Utilities', 'Entertainment', 'Health']
CITY_TIERS = ['1', '2', '3', '4']

# --- Main Seeding Logic ---
def get_db_connection():
    """Establishes a connection to the SQLite database."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def clear_existing_data(cursor):
    """Clears all relevant tables before inserting new data."""
    print("Clearing existing data...")
    cursor.execute("DELETE FROM upi_transactions_raw")
    cursor.execute("DELETE FROM credit_card_transactions_raw")
    cursor.execute("DELETE FROM segment_catalog")
    cursor.execute("DELETE FROM rule_table")
    
    # Drop all old segment output tables
    tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'segment_output_%'").fetchall()
    for table in tables:
        print(f"Dropping old segment table: {table['name']}")
        cursor.execute(f"DROP TABLE IF EXISTS {table['name']}")
    print("Data cleared.")

def create_transactions(num_users, transactions_per_user):
    """Generates a mix of UPI and credit card transactions for the given users."""
    fake = Faker()
    upi_transactions = []
    cc_transactions = []
    users = list(range(1, num_users + 1))

    for user_id in users:
        for _ in range(transactions_per_user):
            amount = round(random.uniform(10.0, 2000.0), 2)
            transaction_date = fake.date_time_between(start_date=START_DATE, end_date=END_DATE)
            category = random.choice(CATEGORIES)
            city_tier = random.choice(CITY_TIERS)
            transaction_id = str(uuid.uuid4())

            if random.random() > 0.4: # 60% UPI
                upi_transactions.append((
                    transaction_id, user_id, amount, 
                    transaction_date.strftime("%Y-%m-%d %H:%M:%S"), category, city_tier
                ))
            else: # 40% Credit Card
                cc_transactions.append((
                    transaction_id, user_id, amount, 
                    transaction_date.strftime("%Y-%m-%d %H:%M:%S"), category, city_tier
                ))
    return upi_transactions, cc_transactions

def insert_data(cursor, upi_data, cc_data):
    """Inserts the generated transaction data into the database."""
    print(f"Inserting {len(upi_data)} UPI transactions...")
    cursor.executemany(
        "INSERT INTO upi_transactions_raw (transaction_id, user_id, amount, transaction_date, category, city_tier) VALUES (?, ?, ?, ?, ?, ?)",
        upi_data
    )
    print(f"Inserting {len(cc_data)} credit card transactions...")
    cursor.executemany(
        "INSERT INTO credit_card_transactions_raw (transaction_id, user_id, amount, transaction_date, category, city_tier) VALUES (?, ?, ?, ?, ?, ?)",
        cc_data
    )
    print("Data insertion complete.")

def main():
    """Main function to run the database seeding process."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        clear_existing_data(cursor)
        upi_data, cc_data = create_transactions(NUM_USERS, TRANSACTIONS_PER_USER)
        insert_data(cursor, upi_data, cc_data)
        conn.commit()
        print("\nDatabase has been successfully seeded with realistic data!")
        print(f"Total Users: {NUM_USERS}")
        print(f"Total UPI Transactions: {len(upi_data)}")
        print(f"Total Credit Card Transactions: {len(cc_data)}")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == '__main__':
    main()
