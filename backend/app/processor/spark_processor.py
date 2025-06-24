# app/processor/spark_processor.py
import os
import sys
import json
import logging
from typing import Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from .base_processor import BaseSegmentProcessor
from .segment_operations import SegmentOperations
from ..models.rule_engine import SegmentCatalog

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class SparkSegmentProcessor(BaseSegmentProcessor):
    def __init__(self, db_session, rule_id: int, spark_session: SparkSession = None):
        super().__init__(db_session, rule_id)
        self.spark = spark_session or self._create_spark_session()
        self.segment_ops = SegmentOperations(self.spark)
        
        backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        db_path = os.path.join(backend_dir, "db", "audience_manager.db")
        self.jdbc_url = f"jdbc:sqlite:{db_path}"
        logger.info(f"Using database at: {db_path}")

        # Load the full segment metadata object upon initialization
        self.segment = self._get_segment_metadata_obj()

    def _get_segment_metadata_obj(self) -> Optional[SegmentCatalog]:
        """Fetches the full SegmentCatalog SQLAlchemy object for the current rule_id."""
        try:
            segment = self.db.query(SegmentCatalog).filter_by(rule_id=self.rule_id).one_or_none()
            if not segment:
                logger.error(f"No SegmentCatalog entry found for rule_id: {self.rule_id}")
                return None
            return segment
        except Exception as e:
            logger.error(f"Error fetching SegmentCatalog for rule {self.rule_id}: {e}", exc_info=True)
            return None

    def _create_spark_session(self) -> SparkSession:
        backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        jdbc_path = os.path.join(backend_dir, "jars", "sqlite-jdbc-3.45.3.0.jar")
        logger.info(f"Using JDBC JAR at: {jdbc_path}")
        
        return SparkSession.builder \
            .appName(f"SegmentProcessor-Rule-{self.rule_id}") \
            .config("spark.driver.extraClassPath", jdbc_path) \
            .config("spark.executor.extraClassPath", jdbc_path) \
            .config("spark.sql.debug.maxToStringFields", "100") \
            .getOrCreate()

    def _get_segment_data(self) -> Optional[DataFrame]:
        """Determines the data retrieval strategy based on segment dependencies."""
        logger.info(f"Getting segment data for rule {self.rule_id}")

        if not self.segment:
            logger.error(f"Segment metadata for rule {self.rule_id} not loaded.")
            return None

        # 1. Check for dependencies to reuse existing segments
        if self.segment.depends_on and self.segment.operation:
            logger.info(f"[REUSE] Compound rule detected for rule {self.rule_id}. Reusing existing segments.")
            logger.info(f"[REUSE] Operation: {self.segment.operation}, Depends On: {self.segment.depends_on}")

            try:
                parent_rule_ids = json.loads(self.segment.depends_on)
            except (json.JSONDecodeError, TypeError):
                logger.error(f"[REUSE] Could not parse 'depends_on' field: {self.segment.depends_on}")
                return None

            parent_segment_dfs = []
            for parent_id in parent_rule_ids:
                df = self._get_segment_data_from_output(parent_id)
                if df:
                    logger.info(f"[REUSE] Successfully loaded parent segment from rule {parent_id}.")
                    parent_segment_dfs.append(df)
                else:
                    logger.error(f"[REUSE] Failed to load parent segment from rule {parent_id}. Aborting.")
                    return None

            if len(parent_segment_dfs) < 2:
                logger.error(f"[REUSE] Not enough parent segments ({len(parent_segment_dfs)}) could be loaded for combination.")
                return None

            return self.segment_ops.combine_segments(parent_segment_dfs, self.segment.operation)

        # 2. If no dependencies, process as a base rule using its SQL query
        elif self.segment.sql_query:
            logger.info(f"[BASE] Base rule detected for rule {self.rule_id}. Processing with SQL query.")
            logger.debug(f"[BASE] SQL Query: {self.segment.sql_query}")
            try:
                schema = "user_id LONG, total_transactions LONG, total_spent DECIMAL(20,2), transaction_types STRING"
                df = self.spark.read \
                    .format("jdbc") \
                    .option("url", self.jdbc_url) \
                    .option("query", f"({self.segment.sql_query}) as subquery") \
                    .option("driver", "org.sqlite.JDBC") \
                    .option("customSchema", schema) \
                    .load()
                
                return df.filter(col("user_id").isNotNull())
            except Exception as e:
                logger.error(f"[BASE] Error executing SQL query for rule {self.rule_id}: {e}", exc_info=True)
                return None

        # 3. Fallback/Error
        logger.error(f"No valid processing method for rule {self.rule_id}. No dependencies and no SQL query.")
        return None
        
    def _get_segment_data_from_output(self, rule_id: int) -> Optional[DataFrame]:
        """Get data from an existing segment output table by its rule ID."""
        try:
            table_name = f"segment_output_{rule_id}"
            logger.info(f"Loading data from dependent table: {table_name}")
            # Use a query with an explicit schema to avoid type inference issues with SQLite JDBC
            schema = "user_id LONG, total_transactions DECIMAL(20,2), total_spent DECIMAL(20,2), transaction_types STRING"
            return self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("query", f"(SELECT * FROM {table_name}) as subquery") \
                .option("driver", "org.sqlite.JDBC") \
                .option("customSchema", schema) \
                .load()
        except Exception as e:
            logger.warning(f"Failed to load segment for rule {rule_id}: {e}")
            return None
    
    def _update_segment_metadata(self, row_count: int) -> None:
        engine = None
        db_session = None
        try:
            backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            db_path = os.path.join(backend_dir, 'db', 'audience_manager.db')
            db_url = f"sqlite:///{db_path}"
            engine = create_engine(db_url)
            Session = sessionmaker(bind=engine)
            db_session = Session()
            
            logger.info(f"Updating metadata for rule {self.rule_id} with row_count {row_count}")
            
            db_session.execute(
                text("""
                    UPDATE segment_catalog 
                    SET row_count = :row_count, last_refreshed_at = :now
                    WHERE rule_id = :rule_id
                """),
                {'row_count': row_count, 'now': datetime.utcnow(), 'rule_id': self.rule_id}
            )
            db_session.commit()
            logger.info(f"Successfully updated metadata for rule {self.rule_id}")
        except Exception as e:
            logger.error(f"Failed to update segment metadata for rule {self.rule_id}: {e}", exc_info=True)
            if db_session: db_session.rollback()
        finally:
            if db_session: db_session.close()
            if engine: engine.dispose()

    def _save_segment_output(self, df: DataFrame) -> bool:
        output_table = f"segment_output_{self.rule_id}"
        engine = None
        try:
            backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            db_path = os.path.join(backend_dir, 'db', 'audience_manager.db')
            db_url = f"sqlite:///{db_path}"
            engine = create_engine(db_url)
            
            logger.info(f"Dropping existing table `{output_table}` if it exists...")
            with engine.connect() as connection:
                with connection.begin():
                    connection.execute(text(f"DROP TABLE IF EXISTS {output_table}"))
            logger.info(f"Successfully dropped table `{output_table}`.")
        except Exception as e:
            logger.error(f"Could not drop table `{output_table}`: {e}", exc_info=True)
            return False
        finally:
            if engine: engine.dispose()

        try:
            # Define column types for the target table to ensure schema consistency
            column_types = "user_id BIGINT, total_transactions DECIMAL(20,2), total_spent DECIMAL(20,2), transaction_types STRING"
            df.write \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", output_table) \
                .option("driver", "org.sqlite.JDBC") \
                .option("createTableColumnTypes", column_types) \
                .mode("overwrite") \
                .save()
            return True
        except Exception as e:
            logger.error(f"Failed to save segment output to {output_table}: {e}", exc_info=True)
            return False

    def process(self) -> bool:
        try:
            logger.info(f"Processing segment for rule {self.rule_id}")
            df = self._get_segment_data()
            
            row_count = 0
            if df is None or df.rdd.isEmpty():
                logger.warning(f"No data found for rule {self.rule_id}. Creating an empty segment output.")
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
                row_count = df.count()
                
            if not self._save_segment_output(df):
                logger.error(f"Failed to save output for rule {self.rule_id}")
                return False
                
            self._update_segment_metadata(row_count)
            logger.info(f"Successfully processed segment for rule {self.rule_id} with {row_count} rows")
            return True
        except Exception as e:
            logger.error(f"Error processing segment for rule {self.rule_id}: {e}", exc_info=True)
            return False
        finally:
            if hasattr(self, 'spark') and self.spark.getActiveSession():
                self.spark.stop()

if __name__ == "__main__":
    import argparse
    db_session = None
    try:
        parser = argparse.ArgumentParser(description='Process a segment rule using Spark')
        parser.add_argument('--rule_id', type=int, required=True, help='ID of the rule to process')
        args = parser.parse_args()
        
        backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        db_path = os.path.join(backend_dir, 'db', 'audience_manager.db')
        db_url = f"sqlite:///{db_path}"
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        db_session = Session()
        
        logger.info(f"Starting segment processor for rule_id: {args.rule_id}")
        processor = SparkSegmentProcessor(db_session, args.rule_id)
        success = processor.process()
        
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"CRITICAL ERROR in segment processor __main__: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if db_session:
            db_session.close()