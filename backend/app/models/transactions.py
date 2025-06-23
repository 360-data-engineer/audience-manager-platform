# app/models/transactions.py
from .. import db

class UPITransaction(db.Model):
    __tablename__ = 'upi_transactions_raw'
    
    id = db.Column(db.Integer, primary_key=True)
    transaction_id = db.Column(db.String(100), unique=True, nullable=False)
    user_id = db.Column(db.String(100), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    transaction_date = db.Column(db.DateTime, nullable=False)
    merchant_name = db.Column(db.String(200))
    category = db.Column(db.String(100))
    city_tier = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=db.func.current_timestamp())

    def to_dict(self):
        return {
            'id': self.id,
            'transaction_id': self.transaction_id,
            'user_id': self.user_id,
            'amount': self.amount,
            'transaction_date': self.transaction_date.isoformat(),
            'merchant_name': self.merchant_name,
            'category': self.category,
            'city_tier': self.city_tier,
            'created_at': self.created_at.isoformat()
        }

class CreditCardPayment(db.Model):
    __tablename__ = 'credit_card_transactions_raw'
    
    id = db.Column(db.Integer, primary_key=True)
    transaction_id = db.Column(db.String(100), unique=True, nullable=False)
    user_id = db.Column(db.String(100), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    transaction_date = db.Column(db.DateTime, nullable=False)
    merchant_name = db.Column(db.String(200))
    category = db.Column(db.String(100))
    city_tier = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=db.func.current_timestamp())

    def to_dict(self):
        return {
            'id': self.id,
            'transaction_id': self.transaction_id,
            'user_id': self.user_id,
            'amount': self.amount,
            'transaction_date': self.transaction_date.isoformat(),
            'merchant_name': self.merchant_name,
            'category': self.category,
            'city_tier': self.city_tier,
            'created_at': self.created_at.isoformat()
        }