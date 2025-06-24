import json
import logging
from app import create_app
from app.models.rule_engine import Rule

# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """
    Updates the 'conditions' for specific rules to ensure they are in the correct JSON format.
    This is a one-off script to fix data inconsistencies.
    """
    app = create_app()
    with app.app_context():
        from app import db

        # Define the correct conditions for the rules based on their descriptions
        # These correspond to the rules created on the UI
        rule_definitions = {
            1: {'type': 'transaction_amount', 'operator': '>', 'value': 1000},
            2: {'city_tier_in': ['Tier 1']},
            3: {'type': 'transaction_amount', 'operator': '>', 'value': 1000, 'city_tier_in': ['Tier 1']}
        }

        logger.info("Starting to fix rule conditions...")

        try:
            for rule_id, conditions in rule_definitions.items():
                rule = db.session.query(Rule).filter_by(id=rule_id).first()
                if rule:
                    logger.info(f"Updating conditions for Rule ID: {rule_id} ({rule.rule_name})")
                    rule.conditions = json.dumps(conditions)
                else:
                    logger.warning(f"Rule ID: {rule_id} not found. Skipping.")
            
            db.session.commit()
            logger.info("✅ Successfully updated rule conditions.")

        except Exception as e:
            logger.error(f"A critical error occurred while updating conditions: {e}", exc_info=True)
            db.session.rollback()

if __name__ == "__main__":
    main()
