import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import create_app, db
from app.models.rule_engine import Rule, SegmentCatalog
from app.utils.rule_parser import RuleParser

def backfill_sql_queries():
    """Backfills the sql_query for existing SegmentCatalog entries."""
    app = create_app()
    with app.app_context():
        print("Starting backfill process for SegmentCatalog sql_query...")
        segments = SegmentCatalog.query.all()
        updated_count = 0

        if not segments:
            print("No segments found in SegmentCatalog. Nothing to do.")
            return

        for segment in segments:
            if segment.rule_id and not segment.sql_query:
                rule = Rule.query.get(segment.rule_id)
                if rule:
                    try:
                        sql_query = RuleParser.generate_segment_sql(rule.id, rule.conditions)
                        segment.sql_query = sql_query
                        db.session.add(segment)
                        updated_count += 1
                        print(f"Generated and updated sql_query for segment_id: {segment.id}")
                    except Exception as e:
                        print(f"Error generating SQL for rule_id {rule.id}: {e}")
                else:
                    print(f"Rule with id {segment.rule_id} not found for segment_id: {segment.id}")
            elif segment.sql_query:
                print(f"sql_query already exists for segment_id: {segment.id}. Skipping.")

        if updated_count > 0:
            db.session.commit()
            print(f"\nSuccessfully updated {updated_count} SegmentCatalog entries.")
        else:
            print("\nNo entries needed updating.")

        print("Backfill process completed.")

if __name__ == "__main__":
    backfill_sql_queries()
