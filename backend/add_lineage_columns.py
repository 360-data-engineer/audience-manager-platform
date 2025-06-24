import logging
from app import create_app
from app.models.rule_engine import db

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """
    Adds 'depends_on' and 'operation' columns to the 'segment_catalog' table.
    """
    app = create_app()
    with app.app_context():
        try:
            # Use raw SQL for broader compatibility, especially with SQLite
            with db.engine.connect() as connection:
                logger.info("Checking for 'depends_on' column in 'segment_catalog'...")
                # Use a transaction for safety
                trans = connection.begin()
                try:
                    connection.execute(db.text('ALTER TABLE segment_catalog ADD COLUMN depends_on TEXT'))
                    logger.info("✅ Column 'depends_on' added successfully.")
                except Exception as e:
                    if "duplicate column name" in str(e):
                        logger.warning("⚠️ Column 'depends_on' already exists. Skipping.")
                    else:
                        raise

                logger.info("Checking for 'operation' column in 'segment_catalog'...")
                try:
                    connection.execute(db.text('ALTER TABLE segment_catalog ADD COLUMN operation VARCHAR(50)'))
                    logger.info("✅ Column 'operation' added successfully.")
                except Exception as e:
                    if "duplicate column name" in str(e):
                        logger.warning("⚠️ Column 'operation' already exists. Skipping.")
                    else:
                        raise
                
                trans.commit()
                logger.info("Database migration completed successfully.")

        except Exception as e:
            logger.error(f"An error occurred during migration: {e}", exc_info=True)

if __name__ == "__main__":
    main()
