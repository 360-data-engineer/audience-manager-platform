#!/usr/bin/env python3
"""
Script to fix and update the segment_catalog table schema in SQLite.
Handles adding missing columns and ensuring proper schema for the rule engine.
"""

import os
import sys
from pathlib import Path
from sqlalchemy import inspect, text, Table, MetaData

# Add the current directory to the Python path
sys.path.append(str(Path(__file__).parent))

from app import create_app, db
from app.models.rule_engine import SegmentCatalog

def fix_segment_catalog():
    """
    Fixes the segment_catalog table schema by:
    1. Creating the table if it doesn't exist
    2. Adding missing columns (sql_query, table_name, row_count, rule_id)
    3. Setting proper defaults and constraints
    """
    app = create_app()
    
    with app.app_context():
        print("Fixing segment_catalog table...")
        
        # Enable foreign key constraints for SQLite
        db.session.execute(text('PRAGMA foreign_keys = ON'))
        
        # Create an inspector to check table existence
        inspector = inspect(db.engine)
        
        # Check if segment_catalog exists
        if not inspector.has_table('segment_catalog'):
            print("Creating segment_catalog table...")
            db.create_all()
            print("segment_catalog table created successfully!")
        else:
            print("segment_catalog table exists, checking schema...")
            
            # Get current columns
            columns = {col['name'] for col in inspector.get_columns('segment_catalog')}
            print(f"Current columns: {', '.join(columns)}")
            
            # Check if we need to add sql_query column
            if 'sql_query' not in columns:
                print("Adding sql_query column...")
                try:
                    # Get existing data
                    result = db.session.execute(text('SELECT * FROM segment_catalog'))
                    old_data = [dict(row) for row in result.mappings()]
                    
                    # Create a new table with the updated schema
                    db.session.execute(text('''
                        CREATE TABLE segment_catalog_new (
                            id INTEGER NOT NULL,
                            segment_name VARCHAR(255) NOT NULL,
                            description TEXT,
                            table_name VARCHAR(255) NOT NULL,
                            row_count INTEGER,
                            rule_id INTEGER,
                            refresh_frequency VARCHAR(50),
                            last_refreshed_at DATETIME,
                            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                            sql_query TEXT DEFAULT '',
                            is_active BOOLEAN DEFAULT 1,
                            target_table VARCHAR(255),
                            PRIMARY KEY (id),
                            UNIQUE (segment_name),
                            FOREIGN KEY(rule_id) REFERENCES rule_table (id)
                        )
                    '''))
                    
                    # Re-insert the old data with default values for new columns
                    for row in old_data:
                        # Set default values for required fields
                        row.setdefault('sql_query', '')
                        row.setdefault('table_name', f"segment_output_{row.get('id', 'temp')}")
                        row.setdefault('row_count', 0)
                        row.setdefault('refresh_frequency', 'DAILY')
                        row.setdefault('is_active', 1)
                        row.setdefault('target_table', '')
                        
                        # Prepare columns and values
                        columns = [col for col in row.keys() if col != 'id' or 'id' not in columns]
                        placeholders = ', '.join(['?'] * len(columns))
                        values = [row[col] for col in columns]
                        
                        # Build and execute the insert statement
                        sql = f"INSERT INTO segment_catalog_new ({', '.join(columns)}) VALUES ({placeholders})"
                        db.session.execute(text(sql), values)
                    
                    # Drop the old table and rename the new one
                    db.session.execute(text('DROP TABLE segment_catalog'))
                    db.session.execute(text('ALTER TABLE segment_catalog_new RENAME TO segment_catalog'))
                    db.session.commit()
                    print("Added sql_query column and updated schema successfully!")
                    
                except Exception as e:
                    db.session.rollback()
                    print(f"Error updating schema: {e}")
                    raise
        
        # Verify the final schema
        print("\nFinal schema for segment_catalog:")
        result = db.session.execute(text("PRAGMA table_info(segment_catalog)"))
        for row in result:
            print(f"Column: {row[1]}, Type: {row[2]}, Not Null: {row[3]}, Default: {row[4]}, PK: {row[5]}")
        
        # Verify foreign key constraints
        print("\nForeign key constraints:")
        result = db.session.execute(text("PRAGMA foreign_key_list(segment_catalog)"))
        for row in result:
            print(f"FK: {row[3]} references {row[2]}.{row[4]}")
        
        print("\nDatabase schema verification completed successfully!")

if __name__ == '__main__':
    fix_segment_catalog()