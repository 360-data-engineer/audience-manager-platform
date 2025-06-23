# migrate_db.py
import os
import sys
from pathlib import Path
from sqlalchemy import inspect

# Add the current directory to the Python path
sys.path.append(str(Path(__file__).parent))

from app import create_app, db
from app.models.rule_engine import Rule, SegmentCatalog

def migrate_database():
    app = create_app()
    
    with app.app_context():
        print("Starting database migration...")
        
        # Enable foreign key constraints for SQLite
        db.session.execute('PRAGMA foreign_keys = ON')
        
        # Create an inspector to check table existence
        inspector = inspect(db.engine)
        
        # Check if rule_table exists
        if not inspector.has_table('rule_table'):
            print("Creating rule_table...")
            db.create_all()
            print("Database tables created successfully!")
            return
        
        # Check if rule_table has the schedule column
        columns = [col['name'] for col in inspector.get_columns('rule_table')]
        has_schedule = 'schedule' in columns
        
        if not has_schedule:
            print("Updating rule_table schema...")
            # Create a backup of the existing table
            db.session.execute("""
                CREATE TABLE IF NOT EXISTS rule_table_backup AS 
                SELECT * FROM rule_table
            """)
            
            # Drop the existing table
            db.session.execute("DROP TABLE IF EXISTS rule_table")
            
            # Recreate the table with the new schema
            db.create_all()
            
            # Copy data back from backup
            db.session.execute("""
                INSERT INTO rule_table (id, rule_name, description, conditions, is_active, created_at, updated_at)
                SELECT 
                    id, 
                    rule_name, 
                    description, 
                    '{}' as conditions, 
                    COALESCE(is_active, 1) as is_active,
                    created_at,
                    datetime('now') as updated_at
                FROM rule_table_backup
            """)
            
            # Drop the backup table
            db.session.execute("DROP TABLE IF EXISTS rule_table_backup")
            print("rule_table updated successfully!")
        else:
            print("rule_table is already up to date")
        
        # Check if segment_catalog exists
        if not inspector.has_table('segment_catalog'):
            print("Creating segment_catalog table...")
            SegmentCatalog.__table__.create(db.engine)
            print("segment_catalog table created successfully!")
        else:
            print("segment_catalog table already exists")
        
        db.session.commit()
        print("Database migration completed successfully!")

if __name__ == '__main__':
    migrate_database()