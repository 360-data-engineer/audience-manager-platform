import sqlite3
from pathlib import Path

def init_database():
    db_path = Path(__file__).parent / 'db' / 'audience_manager.db'
    sql_path = Path(__file__).parent / 'db' / 'init_db.sql'
    
    # Create database directory if it doesn't exist
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Connect to SQLite database (creates it if it doesn't exist)
    print(f"Initializing database at: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Read and execute the SQL script
        print("Reading SQL initialization script...")
        with open(sql_path, 'r') as sql_file:
            sql_script = sql_file.read()
        
        print("Executing SQL script...")
        cursor.executescript(sql_script)
        conn.commit()
        
        # Verify tables were created
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print("\nDatabase initialized successfully!")
        print("\nTables created:")
        for table in tables:
            print(f"- {table[0]}")
            
    except Exception as e:
        print(f"\nError initializing database: {e}")
        conn.rollback()
    finally:
        conn.close()
        print(f"\nDatabase connection closed. You can find your database at: {db_path}")

if __name__ == "__main__":
    init_database()
