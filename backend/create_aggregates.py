import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

# Database path
DB_PATH = Path(__file__).parent / 'db' / 'audience_manager.db'

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

def create_upi_aggregates(conn):
    """Create aggregate data for UPI transactions."""
    print("Creating UPI transaction aggregates...")
    cursor = conn.cursor()
    
    # Calculate date ranges
    end_date = datetime.now().date()
    start_date_30d = (end_date - timedelta(days=30)).strftime('%Y-%m-%d')
    start_date_60d = (end_date - timedelta(days=60)).strftime('%Y-%m-%d')
    start_date_90d = (end_date - timedelta(days=90)).strftime('%Y-%m-%d')
    
    # Clear existing aggregate data
    cursor.execute("DELETE FROM upi_transactions_agg")
    
    # 30-day aggregates
    cursor.execute(f"""
        INSERT INTO upi_transactions_agg 
        (user_id, total_transactions, total_amount, last_transaction_date, 
         favorite_category, city_tier, period_start, period_end)
        SELECT 
            user_id,
            COUNT(*) as total_transactions,
            SUM(amount) as total_amount,
            MAX(transaction_date) as last_transaction_date,
            (SELECT category FROM upi_transactions_raw 
             WHERE user_id = t.user_id 
             GROUP BY category 
             ORDER BY COUNT(*) DESC 
             LIMIT 1) as favorite_category,
            MAX(city_tier) as city_tier,
            ? as period_start,
            ? as period_end
        FROM upi_transactions_raw t
        WHERE transaction_date >= ?
        GROUP BY user_id
    """, (start_date_30d, end_date.strftime('%Y-%m-%d'), start_date_30d))
    
    # 60-day aggregates
    cursor.execute(f"""
        INSERT INTO upi_transactions_agg 
        (user_id, total_transactions, total_amount, last_transaction_date, 
         favorite_category, city_tier, period_start, period_end)
        SELECT 
            user_id,
            COUNT(*) as total_transactions,
            SUM(amount) as total_amount,
            MAX(transaction_date) as last_transaction_date,
            (SELECT category FROM upi_transactions_raw 
             WHERE user_id = t.user_id 
             GROUP BY category 
             ORDER BY COUNT(*) DESC 
             LIMIT 1) as favorite_category,
            MAX(city_tier) as city_tier,
            ? as period_start,
            ? as period_end
        FROM upi_transactions_raw t
        WHERE transaction_date >= ?
        GROUP BY user_id
    """, (start_date_60d, end_date.strftime('%Y-%m-%d'), start_date_60d))
    
    # 90-day aggregates
    cursor.execute(f"""
        INSERT INTO upi_transactions_agg 
        (user_id, total_transactions, total_amount, last_transaction_date, 
         favorite_category, city_tier, period_start, period_end)
        SELECT 
            user_id,
            COUNT(*) as total_transactions,
            SUM(amount) as total_amount,
            MAX(transaction_date) as last_transaction_date,
            (SELECT category FROM upi_transactions_raw 
             WHERE user_id = t.user_id 
             GROUP BY category 
             ORDER BY COUNT(*) DESC 
             LIMIT 1) as favorite_category,
            MAX(city_tier) as city_tier,
            ? as period_start,
            ? as period_end
        FROM upi_transactions_raw t
        WHERE transaction_date >= ?
        GROUP BY user_id
    """, (start_date_90d, end_date.strftime('%Y-%m-%d'), start_date_90d))
    
    conn.commit()
    print("✓ Created UPI transaction aggregates")

def main():
    print("Starting aggregate data generation...")
    conn = create_connection()
    
    if conn is not None:
        try:
            create_upi_aggregates(conn)
            print("\n✅ Aggregate data generation completed successfully!")
            
            # Print sample of the aggregate data
            cursor = conn.cursor()
            cursor.execute("""
                SELECT user_id, period_start, period_end, total_transactions, 
                       total_amount, favorite_category, city_tier
                FROM upi_transactions_agg
                ORDER BY user_id, period_start
                LIMIT 5
            """)
            
            print("\nSample of generated aggregate data:")
            for row in cursor.fetchall():
                print(f"User: {row['user_id']}, Period: {row['period_start']} to {row['period_end']}")
                print(f"  Transactions: {row['total_transactions']}, Amount: ₹{row['total_amount']:,.2f}")
                print(f"  Favorite Category: {row['favorite_category']}, City Tier: {row['city_tier']}\n")
            
        except Exception as e:
            print(f"\n❌ Error generating aggregate data: {e}")
            conn.rollback()
        finally:
            conn.close()
    else:
        print("Error: Could not connect to the database.")

if __name__ == "__main__":
    main()