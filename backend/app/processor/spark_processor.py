# app/processor/spark_processor.py
import os
import sys
import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, expr
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from .base_processor import BaseSegmentProcessor
from .data_sources import DataSourceManager
from .segment_operations import SegmentOperations

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Ensure debug logs are captured

class SparkSegmentProcessor(BaseSegmentProcessor):
    def __init__(self, db_session, rule_id: int, spark_session: SparkSession = None):
        """
        Initialize the SparkSegmentProcessor.
        
        Args:
            db_session: SQLAlchemy database session
            rule_id: ID of the rule to process
            spark_session: Optional existing SparkSession to reuse
        """
        super().__init__(db_session, rule_id)
        self.spark = spark_session or self._create_spark_session()
        self.data_sources = DataSourceManager(self.spark, db_session)
        self.segment_ops = SegmentOperations(self.spark)
        
        # Use a robust, absolute path to the database
        backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        db_path = os.path.join(backend_dir, "db", "audience_manager.db")
        self.jdbc_url = f"jdbc:sqlite:{db_path}"
        logger.info(f"Using database at: {db_path}")
        
    def _create_spark_session(self) -> SparkSession:
        """Create and configure a Spark session with SQLite support"""
        # Construct a robust, absolute path to the JDBC JAR
        backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        jdbc_path = os.path.join(backend_dir, "jars", "sqlite-jdbc-3.45.3.0.jar")
        logger.info(f"Using JDBC JAR at: {jdbc_path}")
        
        return SparkSession.builder \
            .appName(f"SegmentProcessor-Rule-{self.rule_id}") \
            .config("spark.sql.debug.maxToStringFields", "100") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
            
    def _get_segment_data(self) -> Optional[DataFrame]:
        """Get data for the segment using the optimal data source"""
        logger.info(f"Fetching segment data for rule {self.rule_id}")

        # Manually fetch the segment metadata including sql_query to ensure it's available
        engine = None
        db_session = None
        sql_query = None
        try:
            backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            db_path = os.path.join(backend_dir, 'db', 'audience_manager.db')
            db_url = f"sqlite:///{db_path}"
            engine = create_engine(db_url)
            Session = sessionmaker(bind=engine)
            db_session = Session()
            # Fetch the sql_query from the database for the given rule_id
            result = db_session.execute(
                text("SELECT sql_query FROM segment_catalog WHERE rule_id = :rule_id"),
                {'rule_id': self.rule_id}
            ).fetchone()
            if result and result[0]:
                sql_query = result[0]
                logger.info(f"Successfully fetched SQL query for rule {self.rule_id}")
            else:
                logger.warning(f"No SQL query found for rule {self.rule_id} in segment_catalog")
                
        except Exception as e:
            logger.error(f"Failed to fetch sql_query from database: {e}", exc_info=True)
        finally:
            if db_session:
                db_session.close()
            if engine:
                engine.dispose()

        # 1. Check if we have a direct SQL query
        if sql_query:
            logger.info(f"Using SQL query from database for rule {self.rule_id}")
            logger.debug(f"SQL Query: {sql_query}")
            
            try:
                logger.info("Attempting to read data with Spark JDBC...")
                
                # Read the data using the fetched SQL query, wrapped as a subquery
                df = self.spark.read \
                    .format("jdbc") \
                    .option("url", self.jdbc_url) \
                    .option("query", f"({sql_query}) as subquery") \
                    .option("driver", "org.sqlite.JDBC") \
                    .option("customSchema", "user_id LONG, total_transactions LONG, total_spent DECIMAL(20,2), transaction_types STRING") \
                    .load()
                
                logger.info(f"Schema after JDBC read: {df.schema}")
                
                # Filter out any invalid rows early
                df = df.filter(col("user_id").isNotNull())
                
                logger.info(f"Row count after executing query: {df.count()}")
                if df.rdd.isEmpty():
                    logger.warning(f"No data returned from query for rule {self.rule_id}")
                    return None

                logger.info("Sample data:")
                df.show(5, truncate=False)
                
                return df
                
            except Exception as e:
                logger.error(f"Error fetching segment data with SQL query: {str(e)}", exc_info=True)
                return None
        
        logger.warning("No SQL query available. Attempting fallback data sources.")

        # 2. Check if we're combining existing segments
        if 'depends_on' in self.segment_metadata and self.segment_metadata['depends_on']:
            logger.info(f"Processing dependent segments for rule {self.rule_id}")
            segments = []
            for dep_id in self.segment_metadata['depends_on']:
                logger.info(f"Loading dependent segment {dep_id}")
                if dep_df := self._get_segment_data_from_output(dep_id):
                    segments.append(dep_df)
            
            if segments:
                op = self.segment_metadata['conditions'].get('operation', 'union').upper()
                logger.info(f"Combining {len(segments)} segments with operation: {op}")
                return self.segment_ops.combine_segments(segments, op)
        
        # 3. Try to use aggregate data if specified
        if 'aggregate_table' in self.segment_metadata.get('conditions', {}):
            table_name = self.segment_metadata['conditions']['aggregate_table']
            logger.info(f"Loading aggregate data from table: {table_name}")
            if df := self.data_sources.get_aggregate_data(table_name):
                logger.info(f"Successfully loaded aggregate data with {df.count()} rows")
                return df

        logger.error(f"No valid data source could be determined for rule {self.rule_id}")
        return None
        
    def _get_segment_data_from_output(self, segment_id: int) -> Optional[DataFrame]:
        """Get data from an existing segment output table"""
        try:
            table_name = f"segment_output_{segment_id}"
            return self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", table_name) \
                .option("driver", "org.sqlite.JDBC") \
                .load()
        except Exception as e:
            logger.warning(f"Failed to load segment {segment_id}: {str(e)}")
            return None
    
    def _update_segment_metadata(self, row_count: int) -> None:
        """Update the segment catalog with the latest metadata"""
        # This method runs in a separate process, so we need a new DB session
        engine = None
        db_session = None
        try:
            # Use a robust, absolute path to the database
            backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            db_path = os.path.join(backend_dir, 'db', 'audience_manager.db')
            db_url = f"sqlite:///{db_path}"
            engine = create_engine(db_url)
            Session = sessionmaker(bind=engine)
            db_session = Session()
            
            logger.info(f"Updating metadata for rule {self.rule_id} with row_count {row_count}")
            
            # Use text() for literal SQL
            from sqlalchemy import text
            db_session.execute(
                text("""
                    UPDATE segment_catalog 
                    SET row_count = :row_count, last_refreshed_at = :now
                    WHERE rule_id = :rule_id
                """),
                {
                    'row_count': row_count,
                    'now': datetime.utcnow(),
                    'rule_id': self.rule_id
                }
            )
            db_session.commit()
            logger.info(f"Successfully updated metadata for rule {self.rule_id}")

        except Exception as e:
            logger.error(f"Failed to update segment metadata for rule {self.rule_id}: {e}", exc_info=True)
            if db_session:
                db_session.rollback()
        finally:
            if db_session:
                db_session.close()
            if engine:
                engine.dispose()

    def _save_segment_output(self, df: DataFrame) -> bool:
        """Save the processed segment to the output table, dropping the old table first."""
        output_table = f"segment_output_{self.rule_id}"
        engine = None

        # Manually drop the table to ensure a clean slate, as Spark's overwrite can be unreliable with schema changes.
        try:
            backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            db_path = os.path.join(backend_dir, 'db', 'audience_manager.db')
            db_url = f"sqlite:///{db_path}"
            engine = create_engine(db_url)
            
            logger.info(f"Dropping existing table `{output_table}` if it exists...")
            with engine.connect() as connection:
                trans = connection.begin()
                connection.execute(text(f"DROP TABLE IF EXISTS {output_table}"))
                trans.commit()
            logger.info(f"Successfully dropped table `{output_table}`.")
        except Exception as e:
            logger.error(f"Could not drop table `{output_table}`: {e}", exc_info=True)
            return False
        finally:
            if engine:
                engine.dispose()

        try:
            # Ensure we have the required columns with proper types
            from pyspark.sql.functions import col
            from pyspark.sql.types import StringType, LongType, DecimalType
            
            # Create a new DataFrame with explicit schema
            columns = []
            if 'user_id' in df.columns:
                columns.append(col('user_id').cast(LongType()).alias('user_id'))
            if 'total_transactions' in df.columns:
                columns.append(col('total_transactions').cast(DecimalType(20, 2)).alias('total_transactions'))
            if 'total_spent' in df.columns:
                columns.append(col('total_spent').cast(DecimalType(20, 2)).alias('total_spent'))
            if 'transaction_types' in df.columns:
                columns.append(col('transaction_types').cast(StringType()).alias('transaction_types'))
            
            if not columns:
                logger.error("No valid columns found in the DataFrame to save")
                return False
            
            df_to_save = df.select(columns)
            
            # Write to SQLite using JDBC with explicit schema
            df_to_save.write \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", output_table) \
                .option("driver", "org.sqlite.JDBC") \
                .option("createTableColumnTypes", "user_id BIGINT, total_transactions DECIMAL(20,2), total_spent DECIMAL(20,2), transaction_types STRING") \
                .mode("overwrite") \
                .save()
            
            return True
        except Exception as e:
            logger.error(f"Failed to save segment output: {str(e)}", exc_info=True)
            return False

    
    def process(self) -> bool:
        """Process the segment, handle empty results gracefully, and save the output."""
        try:
            logger.info(f"Processing segment for rule {self.rule_id}")
            
            # Get the data
            df = self._get_segment_data()
            
            row_count = 0
            
            # Gracefully handle cases where the query returns no results
            if df is None or df.rdd.isEmpty():
                logger.warning(f"No data found for rule {self.rule_id}. Creating an empty segment output.")
                
                # Define the schema for an empty DataFrame to ensure the output table is created correctly
                from pyspark.sql.types import StructType, StructField, LongType, DecimalType, StringType
                schema = StructType([
                    StructField("user_id", LongType(), True),
                    StructField("total_transactions", DecimalType(20, 2), True),
                    StructField("total_spent", DecimalType(20, 2), True),
                    StructField("transaction_types", StringType(), True)
                ])
                df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)
                row_count = 0
            else:
                # Apply any additional filters/transformations if data is present
                if 'filters' in self.segment_metadata.get('conditions', {}):
                    for column, condition in self.segment_metadata['conditions']['filters'].items():
                        df = df.filter(f"{column} {condition}")
                row_count = df.count()
                
            # Save the resulting DataFrame (even if empty)
            if not self._save_segment_output(df):
                logger.error(f"Failed to save output for rule {self.rule_id}")
                return False
                
            # Update metadata with the final row count
            self._update_segment_metadata(row_count)
            
            logger.info(f"Successfully processed segment for rule {self.rule_id} with {row_count} rows")
            return True
            
        except Exception as e:
            logger.error(f"Error processing segment for rule {self.rule_id}: {str(e)}", exc_info=True)
            return False
        finally:
            # Only stop the Spark session if we created it
            if not hasattr(self, '_spark_session_provided') or not self._spark_session_provided:
                try:
                    self.spark.stop()
                except:
                    pass
    
    def __del__(self):
        """Ensure Spark session is stopped when the processor is destroyed"""
        if hasattr(self, 'spark'):
            try:
                self.spark.stop()
            except Exception as e:
                logger.warning(f"Error stopping Spark session: {e}")


if __name__ == "__main__":
    import argparse
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process a segment rule using Spark')
    parser.add_argument('--rule_id', type=int, required=True, help='ID of the rule to process')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    # Set up database connection
    db_session = None
    try:
        # Correctly resolve the absolute path to the database from the script's location
        backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        db_path = os.path.join(backend_dir, 'db', 'audience_manager.db')
        db_url = f"sqlite:///{db_path}"

        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        db_session = Session()
        
        logger.info(f"Starting segment processor for rule_id: {args.rule_id}")
        logger.info(f"Database URL: {db_url}")
        
        # Create and run the processor
        processor = SparkSegmentProcessor(db_session, args.rule_id)
        success = processor.process()
        
        if success:
            logger.info(f"Successfully processed rule {args.rule_id}")
            sys.exit(0) # Explicitly exit with success code
        else:
            logger.error(f"Failed to process rule {args.rule_id}")
            sys.exit(1) # Explicitly exit with failure code
        
    except Exception as e:
        logger.error(f"CRITICAL ERROR in segment processor __main__: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        if db_session:
            db_session.close()
            logger.info("Database session closed")