from datetime import datetime
from .. import db
from .base import BaseModel
import json

class SegmentCatalog(BaseModel):
    __tablename__ = 'segment_catalog'
    
    id = db.Column(db.Integer, primary_key=True)
    segment_name = db.Column(db.String(100), unique=True, nullable=False)
    description = db.Column(db.Text)
    sql_query = db.Column(db.Text, nullable=False)
    target_table = db.Column(db.String(100), nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    refresh_frequency = db.Column(db.String(20), default='DAILY')
    last_refreshed_at = db.Column(db.DateTime)

class Rule(BaseModel):
    __tablename__ = 'rule_table'
    
    id = db.Column(db.Integer, primary_key=True)
    rule_name = db.Column(db.String(100), unique=True, nullable=False)
    description = db.Column(db.Text)
    conditions = db.Column(db.JSON, nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    segment_id = db.Column(db.Integer, db.ForeignKey('segment_catalog.id'))
    segment = db.relationship('SegmentCatalog', backref='rules')
    
    @property
    def conditions_dict(self):
        """Return conditions as a Python dictionary."""
        if isinstance(self.conditions, str):
            return json.loads(self.conditions)
        return self.conditions