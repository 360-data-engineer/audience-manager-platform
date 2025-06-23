import sqlite3
from datetime import datetime, timedelta
import random
from pathlib import Path

# Database path
DB_PATH = Path(__file__).parent / 'db' / 'audience_manager.db'

# Sample data
CITIES = ['Mumbai', 'Delhi', 'Bangalore', 'Hyderabad', 'Chennai', 'Kolkata', 'Pune']
CATEGORIES = ['Shopping', 'Food', 'Entertainment', 'Travel', 'Bills', 'Groceries', 'Transfer']
MERCHANTS = ['Amazon', 'Flipkart', 'Zomato', 'Swiggy', 'Uber', 'Ola', 'DMart', 'BigBasket', 'BookMyShow', 'MakeMyTrip']

def create_connection():
    """Create a database connection to the SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        print(e)
    return conn

def insert_upi_transactions(conn, count=50):
    """Insert sample UPI transactions."""
    print(f"Inserting {count} UPI transactions...")
    cursor = conn.cursor()
    
    for i in range(1, count + 1):
        transaction_date = (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d %H:%M:%S')
        amount = round(random.uniform(100, 50000), 2)
        city_tier = random.randint(1, 3)
        
        cursor.execute("""
            INSERT INTO upi_transactions_raw 
            (transaction_id, user_id, amount, transaction_date, merchant_name, category, city_tier)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            f"UPI{int(datetime.now().timestamp())}{random.randint(1000, 9999)}",
            f"USER{random.randint(1, 20)}",
            amount,
            transaction_date,
            random.choice(MERCHANTS),
            random.choice(CATEGORIES),
            city_tier
        ))
    
    conn.commit()
    print(f"✓ Inserted {count} UPI transactions")

def insert_credit_card_transactions(conn, count=50):
    """Insert sample credit card transactions."""
    print(f"Inserting {count} credit card transactions...")
    cursor = conn.cursor()
    
    for i in range(1, count + 1):
        transaction_date = (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d %H:%M:%S')
        amount = round(random.uniform(500, 100000), 2)
        city_tier = random.randint(1, 3)
        
        cursor.execute("""
            INSERT INTO credit_card_transactions_raw 
            (transaction_id, user_id, amount, transaction_date, merchant_name, category, city_tier)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            f"CC{int(datetime.now().timestamp())}{random.randint(1000, 9999)}",
            f"USER{random.randint(1, 20)}",
            amount,
            transaction_date,
            random.choice(MERCHANTS),
            random.choice(CATEGORIES),
            city_tier
        ))
    
    conn.commit()
    print(f"✓ Inserted {count} credit card transactions")

def insert_segments(conn):
    """Insert sample segments."""
    print("Inserting sample segments...")
    cursor = conn.cursor()
    
    segments = [
        {
            "name": "High Value Customers",
            "description": "Customers with total spend > 50,000 in last 30 days",
            "query": "SELECT user_id FROM upi_transactions_agg WHERE total_amount > 50000 AND period_start >= date('now', '-30 days')",
            "target": "segment_high_value"
        },
        {
            "name": "Frequent Shoppers",
            "description": "Customers with > 20 transactions in last 30 days",
            "query": "SELECT user_id FROM upi_transactions_agg WHERE total_transactions > 20 AND period_start >= date('now', '-30 days')",
            "target": "segment_frequent_shoppers"
        }
    ]
    
    for segment in segments:
        cursor.execute("""
            INSERT INTO segment_catalog 
            (segment_name, description, sql_query, table_name)
            VALUES (?, ?, ?, ?)
        """, (
            segment["name"],
            segment["description"],
            segment["query"],
            segment["target"]
        ))
    
    conn.commit()
    print("✓ Inserted sample segments")

def main():
    print("Starting database seeding...")
    conn = create_connection()
    
    if conn is not None:
        try:
            # Insert sample data
            insert_upi_transactions(conn)
            insert_credit_card_transactions(conn)
            insert_segments(conn)
            
            print("\n✅ Database seeding completed successfully!")
            
        except Exception as e:
            print(f"\n❌ Error seeding database: {e}")
            conn.rollback()
        finally:
            conn.close()
    else:
        print("Error: Could not connect to the database.")

if __name__ == "__main__":
    main()
