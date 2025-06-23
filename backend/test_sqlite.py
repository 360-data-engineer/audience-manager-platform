# test_sqlite.py
import os
from pyspark.sql import SparkSession

def test_sqlite_connection():
    # Get absolute path to the JDBC driver
    jdbc_path = os.path.abspath("jars/sqlite-jdbc-3.45.3.0.jar")
    print(f"Using JDBC driver: {jdbc_path}")
    
    # Initialize Spark with SQLite JDBC driver
    spark = SparkSession.builder \
        .appName("SQLiteTest") \
        .config("spark.jars", jdbc_path) \
        .config("spark.driver.extraClassPath", jdbc_path) \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()
    
    try:
        # Test reading from SQLite
        print("\nReading from segment_catalog table...")
        df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:sqlite:db/audience_manager.db") \
            .option("dbtable", "segment_catalog") \
            .option("driver", "org.sqlite.JDBC") \
            .load()
        
        print("Success! Found tables:")
        df.show(5, truncate=False)
        return True
        
    except Exception as e:
        print(f"\nError: {str(e)}")
        return False
    finally:
        spark.stop()

if __name__ == "__main__":
    test_sqlite_connection()