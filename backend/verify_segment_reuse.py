import logging
import json
import os
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import func

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
backend_dir = os.path.abspath(os.path.dirname(__file__))
db_path = os.path.join(backend_dir, 'db', 'audience_manager.db')
db_uri = f'sqlite:///{db_path}'

engine = create_engine(db_uri)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Re-define the SegmentCatalog model to match the one in the application
class SegmentCatalog(Base):
    __tablename__ = 'segment_catalog'
    id = Column(Integer, primary_key=True)
    rule_id = Column(Integer, ForeignKey('rule_table.id'), nullable=False, unique=True)
    segment_name = Column(String(255), nullable=False, unique=True)
    table_name = Column(String(255), nullable=False, unique=True)
    row_count = Column(Integer, default=0)

    sql_query = Column(Text, nullable=True)
    depends_on = Column(Text, nullable=True)  # JSON array of rule_ids
    operation = Column(String(50), nullable=True) # e.g., INTERSECTION, UNION

def main():
    """
    Verifies that segment reuse has been correctly configured in the database.
    """
    session = Session()
    logger.info("Verifying segment reuse configuration in 'segment_catalog'...")

    try:
        # Fetch the segments for the rules we are interested in
        segment_1 = session.query(SegmentCatalog).filter_by(rule_id=1).first()
        segment_2 = session.query(SegmentCatalog).filter_by(rule_id=2).first()
        segment_3 = session.query(SegmentCatalog).filter_by(rule_id=3).first()

        print("\n--- Verification Results ---")

        # Verify Rule 1 (Base Rule)
        if segment_1 and segment_1.sql_query and not segment_1.depends_on:
            print("✅ Rule 1 (Base): Correctly configured with a direct SQL query.")
        else:
            print("❌ Rule 1 (Base): Configuration is incorrect.")

        # Verify Rule 2 (Base Rule)
        if segment_2 and segment_2.sql_query and not segment_2.depends_on:
            print("✅ Rule 2 (Base): Correctly configured with a direct SQL query.")
        else:
            print("❌ Rule 2 (Base): Configuration is incorrect.")

        # Verify Rule 3 (Composite Rule)
        if segment_3 and not segment_3.sql_query and segment_3.depends_on and segment_3.operation == 'INTERSECTION':
            dependencies = json.loads(segment_3.depends_on)
            if sorted(dependencies) == [1, 2]:
                print(f"✅ Rule 3 (Composite): Correctly configured to reuse Segments {dependencies} via {segment_3.operation}.")
            else:
                print(f"❌ Rule 3 (Composite): Dependencies are incorrect. Expected [1, 2], got {dependencies}.")
        else:
            print("❌ Rule 3 (Composite): Configuration is incorrect. It should be set for reuse.")
        
        print("--------------------------\n")

    except Exception as e:
        logger.error(f"An error occurred during verification: {e}", exc_info=True)
    finally:
        session.close()

if __name__ == "__main__":
    main()
