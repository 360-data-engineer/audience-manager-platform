from flask import jsonify, request
from ..models import UPITransaction, CreditCardPayment
from ..utils.database import db_manager
from . import api_bp
from ..utils.response import paginated_response

@api_bp.route('/transactions/upi', methods=['GET'])
def get_upi_transactions():
    """Get paginated list of UPI transactions."""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    
    query = UPITransaction.query
    return jsonify(paginated_response(query, page=page, per_page=per_page))

@api_bp.route('/transactions/credit-card', methods=['GET'])
def get_credit_card_transactions():
    """Get paginated list of credit card transactions."""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    
    query = CreditCardPayment.query
    return jsonify(paginated_response(query, page=page, per_page=per_page))

@api_bp.route('/transactions/upi/<int:transaction_id>', methods=['GET'])
def get_upi_transaction(transaction_id):
    """Get a single UPI transaction by ID."""
    transaction = UPITransaction.query.get_or_404(transaction_id)
    return jsonify(transaction.to_dict())

@api_bp.route('/transactions/credit-card/<int:transaction_id>', methods=['GET'])
def get_credit_card_transaction(transaction_id):
    """Get a single credit card transaction by ID."""
    transaction = CreditCardPayment.query.get_or_404(transaction_id)
    return jsonify(transaction.to_dict())