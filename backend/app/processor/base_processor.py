# app/processor/base_processor.py
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
import logging

logger = logging.getLogger(__name__)

class BaseSegmentProcessor(ABC):
    def __init__(self, db_session: Session, rule_id: int):
        self.db = db_session
        self.rule_id = rule_id
        self.segment_metadata = self._load_segment_metadata()
        
    def _load_segment_metadata(self) -> Dict[str, Any]:
        """Load metadata for the segment from the database"""
        result = self.db.execute("""
            SELECT * FROM segment_catalog 
            WHERE rule_id = :rule_id
        """, {'rule_id': self.rule_id}).fetchone()
        return dict(result) if result else None
        
    @abstractmethod
    def process(self) -> bool:
        """Process the segment and return success status"""
        pass
        
    def update_metadata(self, row_count: int, table_name: str, sql_query: str) -> None:
        """Update segment metadata in the database"""
        try:
            self.db.execute("""
                UPDATE segment_catalog 
                SET row_count = :row_count,
                    table_name = :table_name,
                    sql_query = :sql_query,
                    last_refreshed_at = CURRENT_TIMESTAMP
                WHERE rule_id = :rule_id
            """, {
                'row_count': row_count,
                'table_name': table_name,
                'sql_query': sql_query,
                'rule_id': self.rule_id
            })
            self.db.commit()
            logger.info(f"Updated metadata for segment from rule {self.rule_id}")
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to update segment metadata: {str(e)}")
            raise