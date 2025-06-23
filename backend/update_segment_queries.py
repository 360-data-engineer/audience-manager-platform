import os
import sys
from datetime import datetime

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from app import create_app, db
from app.models.rule_engine import Rule, SegmentCatalog
from app.utils.rule_parser import RuleParser

app = create_app()

def update_existing_queries():
    """
    Backfills the sql_query for existing segment_catalog entries
    that are missing it.
    """
    with app.app_context():
        print("Starting to update segment queries...")
        
        # Get all rules
        rules = Rule.query.all()
        if not rules:
            print("No rules found in the database.")
            return

        updated_count = 0
        for rule in rules:
            print(f"Processing rule ID: {rule.id}, Name: {rule.rule_name}")
            segment = SegmentCatalog.query.filter_by(rule_id=rule.id).first()
            
            if segment and not segment.sql_query:
                print(f"  -> Segment ID {segment.id} is missing SQL query. Generating...")
                try:
                    # Generate the SQL query from the rule's conditions
                    sql_query = RuleParser.generate_segment_sql(rule.id, rule.conditions)
                    segment.sql_query = sql_query
                    segment.updated_at = datetime.utcnow()
                    updated_count += 1
                    print(f"  -> Successfully generated SQL for segment {segment.id}.")
                except Exception as e:
                    print(f"  -> [ERROR] Failed to generate SQL for rule {rule.id}: {e}")
            elif segment:
                print(f"  -> Segment ID {segment.id} already has a SQL query. Skipping.")
            else:
                print(f"  -> [WARNING] No segment catalog found for rule {rule.id}. Skipping.")

        if updated_count > 0:
            try:
                db.session.commit()
                print(f"\nSuccessfully updated {updated_count} segment queries in the database.")
            except Exception as e:
                db.session.rollback()
                print(f"\n[ERROR] Failed to commit changes to the database: {e}")
        else:
            print("\nNo segment queries needed updating.")

if __name__ == '__main__':
    update_existing_queries()
