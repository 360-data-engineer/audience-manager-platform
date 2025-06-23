#!/usr/bin/env python3
"""
Test script for the SparkSegmentProcessor with detailed logging.
"""
import os
import sys
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.processor.spark_processor import SparkSegmentProcessor

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def test_processor(rule_id: int):
    """Test the SparkSegmentProcessor with the given rule ID."""
    try:
        # Set up database connection
        db_url = "sqlite:///db/audience_manager.db"
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        db_session = Session()
        
        logger.info(f"Testing SparkSegmentProcessor with rule_id: {rule_id}")
        logger.info(f"Database URL: {db_url}")
        
        # Create and run the processor
        logger.info("Initializing SparkSegmentProcessor...")
        processor = SparkSegmentProcessor(db_session, rule_id)
        
        logger.info("Starting segment processing...")
        success = processor.process()
        
        if success:
            logger.info(f"✅ Successfully processed rule {rule_id}")
        else:
            logger.error(f"❌ Failed to process rule {rule_id}")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ Error in test_processor: {str(e)}", exc_info=True)
        return False
    finally:
        if 'db_session' in locals():
            db_session.close()
            logger.info("Database session closed")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Test SparkSegmentProcessor')
    parser.add_argument('--rule_id', type=int, default=1, help='Rule ID to process')
    args = parser.parse_args()
    
    success = test_processor(args.rule_id)
    sys.exit(0 if success else 1)
