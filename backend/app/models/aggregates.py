from datetime import datetime
from .. import db
from .base import BaseModel

class UPITransactionAggregate(BaseModel):
    __tablename__ = 'upi_transactions_agg'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.String(50), nullable=False, index=True)
    total_transactions = db.Column(db.Integer, default=0)
    total_amount = db.Column(db.Numeric(15, 2), default=0)
    last_transaction_date = db.Column(db.DateTime)
    favorite_category = db.Column(db.String(50))
    city_tier = db.Column(db.Integer)
    period_start = db.Column(db.Date, nullable=False)
    period_end = db.Column(db.Date, nullable=False)
    
    __table_args__ = (
        db.UniqueConstraint('user_id', 'period_start', 'period_end', 
                          name='_user_period_uc'),
    )