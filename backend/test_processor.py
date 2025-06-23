# test_processor.py
import logging
from app import create_app, db
from app.processor.spark_processor import SparkSegmentProcessor

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_rule_processing(rule_id: int):
    app = create_app()
    with app.app_context():
        try:
            logger.info(f"Testing SparkSegmentProcessor for rule_id: {rule_id}")
            processor = SparkSegmentProcessor(db.session, rule_id=rule_id)
            success = processor.process()
            
            if success:
                logger.info(f"✅ Successfully processed rule {rule_id}")
            else:
                logger.error(f"❌ Failed to process rule {rule_id}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error processing rule {rule_id}: {str(e)}", exc_info=True)
            return False

if __name__ == "__main__":
    # Test with rule_id=1 (high_value_customers)
    test_rule_processing(rule_id=1)