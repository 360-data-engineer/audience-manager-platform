# jobs/segment_processor_job.py
import sys
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.processor.spark_processor import SparkSegmentProcessor

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def main(rule_id: int, db_uri: str):
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Database setup
    logger.info(f"Connecting to database: {db_uri}")
    engine = create_engine(db_uri)
    Session = sessionmaker(bind=engine)
    db_session = Session()
    
    try:
        # Process the segment
        processor = SparkSegmentProcessor(db_session, rule_id)
        success = processor.process()
        
        if not success:
            logger.error(f"Failed to process segment for rule {rule_id}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        db_session.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit segment_processor_job.py <rule_id> <db_uri>")
        sys.exit(1)
        
    rule_id = int(sys.argv[1])
    db_uri = sys.argv[2]
    main(rule_id, db_uri)