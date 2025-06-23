import json
import logging
from app import create_app
from app.models.rule_engine import Rule
from app.utils.rule_parser import RuleParser

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """
    Iterates through all rules, generates their SQL query based on stored conditions,
    and updates the corresponding entry in the segment_catalog.
    """
    app = create_app()
    with app.app_context():
        from app import db
        
        logger.info("Starting to update SQL queries for all rules...")
        
        try:
            rules = Rule.query.all()
            if not rules:
                logger.warning("No rules found in the database. Nothing to update.")
                return

            for rule in rules:
                logger.info(f"Processing Rule ID: {rule.id}, Name: {rule.rule_name}")
                
                if not rule.conditions:
                    logger.warning(f"Rule ID: {rule.id} has no conditions. Skipping.")
                    continue

                try:
                    # The conditions are stored as a JSON string
                    conditions = json.loads(rule.conditions)
                    
                    # Generate the SQL query using the new parser
                    sql_query = RuleParser.generate_segment_sql(rule.id, conditions)
                    
                    # Update the segment_catalog table for the corresponding rule
                    db.session.execute(
                        """
                        UPDATE segment_catalog 
                        SET sql_query = :sql_query
                        WHERE rule_id = :rule_id
                        """,
                        {'sql_query': sql_query, 'rule_id': rule.id}
                    )
                    logger.info(f"Successfully generated and staged update for Rule ID: {rule.id}")

                except json.JSONDecodeError:
                    logger.error(f"Error decoding conditions for Rule ID: {rule.id}. Conditions: '{rule.conditions}'")
                except Exception as e:
                    logger.error(f"An error occurred while processing Rule ID: {rule.id}: {e}", exc_info=True)

            # Commit all the changes at once
            db.session.commit()
            logger.info("✅ Successfully updated SQL queries for all applicable rules.")

        except Exception as e:
            logger.error(f"A critical error occurred during the update process: {e}", exc_info=True)
            db.session.rollback()

if __name__ == "__main__":
    main()
