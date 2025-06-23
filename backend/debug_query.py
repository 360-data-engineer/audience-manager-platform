import os
import pandas as pd
from sqlalchemy import create_engine, text

# Correctly determine the project's base directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'db', 'audience_manager.db')
DB_URI = f'sqlite:///{DB_PATH}'

def get_sql_query(engine, rule_id):
    """Fetch the SQL query for a given rule_id."""
    with engine.connect() as connection:
        result = connection.execute(
            text("SELECT sql_query FROM segment_catalog WHERE rule_id = :rule_id"),
            {'rule_id': rule_id}
        )
        query = result.scalar_one_or_none()
        return query

def run_debug():
    """
    Connects to the DB, fetches the query for rule 1, executes it,
    and prints the resulting row count and data.
    """
    print(f"Connecting to database at: {DB_PATH}")
    if not os.path.exists(DB_PATH):
        print("ERROR: Database file not found!")
        return

    engine = create_engine(DB_URI)
    
    try:
        # 1. Fetch the query
        rule_id = 1
        sql_query = get_sql_query(engine, rule_id)

        if not sql_query:
            print(f"ERROR: No SQL query found for rule_id {rule_id}")
            return
            
        print("--- SQL Query ---")
        print(sql_query)
        print("-----------------")

        # 2. Execute the query using pandas
        print("Executing query with pandas...")
        df = pd.read_sql_query(sql_query, engine)

        # 3. Print results
        print(f"\n--- Results ---")
        print(f"Number of rows returned: {len(df)}")
        
        if not df.empty:
            print("Sample data:")
            print(df.head())
        print("---------------")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    run_debug()
