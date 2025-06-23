# app/processor/data_sources.py
import os
from typing import Dict, Optional, List, Any
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

class DataSourceManager:
    def __init__(self, spark_session, db_session: Session):
        self.spark = spark_session
        self.db = db_session

        # Determine the absolute path to the database from the script's location
        backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        db_path = os.path.join(backend_dir, 'db', 'audience_manager.db')
        self.jdbc_url = f"jdbc:sqlite:{db_path}"
        logger.info(f"DataSourceManager initialized with JDBC URL: {self.jdbc_url}")
        
    def get_segment_data(self, segment_id: int) -> Optional[Any]:
        """Get data from an existing segment output table"""
        try:
            table_name = f"segment_output_{segment_id}"
            return self.spark.read \
                .format("jdbc") \
                .option("driver", "org.sqlite.JDBC") \
                .option("url", self.jdbc_url) \
                .option("dbtable", table_name) \
                .load()
        except Exception as e:
            logger.warning(f"Failed to load segment {segment_id}: {str(e)}")
            return None
            
    def get_aggregate_data(self, aggregate_name: str) -> Optional[Any]:
        """Get data from aggregate tables"""
        try:
            return self.spark.read \
                .format("jdbc") \
                .option("url", "jdbc:sqlite:db/audience_manager.db") \
                .option("dbtable", aggregate_name) \
                .load()
        except Exception as e:
            logger.warning(f"Failed to load aggregate {aggregate_name}: {str(e)}")
            return None
            
    def get_raw_data(self, table_name: str, columns: List[str] = None) -> Any:
        """Get data from raw tables"""
        columns_str = ", ".join(columns) if columns else "*"
        # Spark's JDBC 'dbtable' option can take a subquery in parentheses
        query = f"(SELECT {columns_str} FROM {table_name}) AS {table_name}_query"
        logger.info(f"Executing raw data query: {query}")
        return self.spark.read \
            .format("jdbc") \
            .option("driver", "org.sqlite.JDBC") \
            .option("url", self.jdbc_url) \
            .option("dbtable", query) \
            .load()