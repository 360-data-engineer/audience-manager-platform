import logging
from app import create_app
from sqlalchemy import text

# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """
    Adds 'depends_on' (JSON TEXT) and 'operation' (TEXT) columns to the 'segment_catalog' table
    to support composite segments.
    """
    app = create_app()
    with app.app_context():
        from app import db
        
        logger.info("Starting database schema migration for segment dependencies...")
        
        try:
            with db.engine.connect() as connection:
                trans = connection.begin()
                # Check if 'depends_on' column exists
                result = connection.execute(text("PRAGMA table_info(segment_catalog);"))
                columns = [row[1] for row in result]

                if 'depends_on' not in columns:
                    connection.execute(text("ALTER TABLE segment_catalog ADD COLUMN depends_on TEXT;"))
                    logger.info(" -> Added 'depends_on' column to 'segment_catalog'.")
                else:
                    logger.info(" -> 'depends_on' column already exists.")

                if 'operation' not in columns:
                    connection.execute(text("ALTER TABLE segment_catalog ADD COLUMN operation TEXT;"))
                    logger.info(" -> Added 'operation' column to 'segment_catalog'.")
                else:
                    logger.info(" -> 'operation' column already exists.")
                trans.commit()
            
            logger.info("✅ Database schema migration completed successfully.")

        except Exception as e:
            logger.error(f"A critical error occurred during the migration: {e}", exc_info=True)

if __name__ == "__main__":
    main()
