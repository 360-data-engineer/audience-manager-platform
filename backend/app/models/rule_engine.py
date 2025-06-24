# app/models/rule_engine.py
from .. import db

class Rule(db.Model):
    """Rule table to store user-defined segmentation rules"""
    __tablename__ = 'rule_table'
    
    id = db.Column(db.Integer, primary_key=True)
    rule_name = db.Column(db.String(255), unique=True, nullable=False)
    description = db.Column(db.Text)
    conditions = db.Column(db.JSON, nullable=False)  # Store filter conditions as JSON
    dependencies = db.Column(db.JSON, nullable=True) # e.g., [1, 2]
    operation = db.Column(db.String(50), nullable=True) # e.g., INTERSECTION, UNION
    is_active = db.Column(db.Boolean, default=True)
    schedule = db.Column(db.String(50), default='DAILY')  # DAILY, HOURLY, etc.
    next_run_at = db.Column(db.DateTime)
    last_run_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())
    
    # Relationships
    segment = db.relationship('SegmentCatalog', backref='rule', uselist=False, lazy=True)
    
    def to_dict(self):
        return {
            'id': self.id,
            'rule_name': self.rule_name,
            'description': self.description,
            'conditions': self.conditions,
            'dependencies': self.dependencies,
            'operation': self.operation,
            'is_active': self.is_active,
            'schedule': self.schedule,
            'next_run_at': self.next_run_at.isoformat() if self.next_run_at else None,
            'last_run_at': self.last_run_at.isoformat() if self.last_run_at else None,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
    
    def save(self):
        db.session.add(self)
        db.session.commit()
        return self

class SegmentCatalog(db.Model):
    """Catalog to track all created segments"""
    __tablename__ = 'segment_catalog'
    
    id = db.Column(db.Integer, primary_key=True)
    segment_name = db.Column(db.String(255), unique=True, nullable=False)
    description = db.Column(db.Text)
    table_name = db.Column(db.String(255), nullable=False)  # segment_output_<id>
    row_count = db.Column(db.Integer, default=0)
    rule_id = db.Column(db.Integer, db.ForeignKey('rule_table.id'))
    sql_query = db.Column(db.Text, nullable=True)  # Store the generated SQL for the Spark job
    
    # Columns for lineage and reuse
    depends_on = db.Column(db.Text, nullable=True)  # JSON array of rule_ids
    operation = db.Column(db.String(50), nullable=True) # e.g., INTERSECTION, UNION

    # Existing columns
    refresh_frequency = db.Column(db.String(50), default='DAILY')
    last_refreshed_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    
    def to_dict(self):
        data = {
            'id': self.id,
            'segment_name': self.segment_name,
            'description': self.description,
            'table_name': self.table_name,
            'row_count': self.row_count,
            'rule_id': self.rule_id,
            'sql_query': self.sql_query,
            'refresh_frequency': self.refresh_frequency,
            'last_refreshed_at': self.last_refreshed_at.isoformat() if self.last_refreshed_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'operation': self.operation,
            'dependencies': []
        }

        if self.depends_on:
            import json
            try:
                parent_rule_ids = json.loads(self.depends_on)
                if parent_rule_ids:
                    parent_segments = db.session.query(SegmentCatalog.segment_name).filter(SegmentCatalog.rule_id.in_(parent_rule_ids)).all()
                    data['dependencies'] = [name for name, in parent_segments]
            except (json.JSONDecodeError, TypeError):
                data['dependencies'] = []
        return data
    
    def save(self):
        db.session.add(self)
        db.session.commit()
        return self