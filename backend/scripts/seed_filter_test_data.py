import random
from datetime import datetime, timedelta
import os
import sys

# Add parent directory to path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import models and db
from app import create_app, db
from app.models.transactions import UPITransaction, CreditCardPayment

def create_test_data():
    app = create_app()
    with app.app_context():
        # Create a test user who will have >10 transactions
        test_user_id = "user_t2_high_value"
        city_tier = 2  # Tier 2 city
        categories = ['shopping', 'food', 'utilities', 'entertainment']
        
        # Create 15 transactions for the test user
        for i in range(15):
            transaction = UPITransaction(
                transaction_id=f"T2_HV_{i}_{int(datetime.utcnow().timestamp())}",
                user_id=test_user_id,
                amount=random.randint(1000, 5000),  # All transactions > 1000
                transaction_date=datetime.utcnow() - timedelta(days=random.randint(0, 30)),
                merchant_name=f"Merchant_{i}",
                category=random.choice(categories),
                city_tier=city_tier
            )
            db.session.add(transaction)
        
        # Also add some non-matching data to test filtering
        for i in range(5):
            # Transactions with amount < 1000
            transaction = UPITransaction(
                transaction_id=f"T2_LV_{i}_{int(datetime.utcnow().timestamp())}",
                user_id=f"user_t2_low_value_{i}",
                amount=random.randint(100, 999),
                transaction_date=datetime.utcnow() - timedelta(days=random.randint(0, 30)),
                merchant_name=f"Merchant_LV_{i}",
                category=random.choice(categories),
                city_tier=city_tier
            )
            db.session.add(transaction)
            
            # Transactions in tier 1 city
            transaction = UPITransaction(
                transaction_id=f"T1_HV_{i}_{int(datetime.utcnow().timestamp())}",
                user_id=f"user_t1_high_value_{i}",
                amount=random.randint(1000, 5000),
                transaction_date=datetime.utcnow() - timedelta(days=random.randint(0, 30)),
                merchant_name=f"Merchant_T1_{i}",
                category=random.choice(categories),
                city_tier=1  # Different tier
            )
            db.session.add(transaction)
        
        # Commit all transactions
        db.session.commit()
        print("Successfully added test data!")

if __name__ == "__main__":
    create_test_data()