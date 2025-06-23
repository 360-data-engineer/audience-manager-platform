# app/processor/segment_operations.py
from pyspark.sql import DataFrame
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)

class SegmentOperations:
    def __init__(self, spark):
        """Initialize with a Spark session."""
        self.spark = spark

    def combine_segments(self, segments: List[DataFrame], operation: str = 'UNION') -> Optional[DataFrame]:
        """Combine multiple segments using the specified operation.
        
        Args:
            segments: List of DataFrames to combine
            operation: One of 'UNION', 'INTERSECTION', or 'DIFFERENCE'
            
        Returns:
            Combined DataFrame or None if no segments provided
        """
        if not segments:
            return None
            
        if len(segments) == 1:
            return segments[0]
            
        try:
            operation = operation.upper()
            if operation == 'UNION':
                # Remove duplicates across all segments
                return segments[0].unionAll(segments[1:]).dropDuplicates()
                
            elif operation == 'INTERSECTION':
                # Start with first segment and keep intersecting rows
                result = segments[0]
                for df in segments[1:]:
                    result = result.intersect(df)
                return result
                
            elif operation == 'DIFFERENCE':
                # Start with first segment and remove rows from others
                result = segments[0]
                for df in segments[1:]:
                    result = result.subtract(df)
                return result
                
            else:
                raise ValueError(f"Unsupported operation: {operation}")
                
        except Exception as e:
            logger.error(f"Error combining segments: {str(e)}")
            raise