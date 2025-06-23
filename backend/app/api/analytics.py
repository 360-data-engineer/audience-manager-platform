from flask import Blueprint, jsonify, request
from datetime import datetime
from ..utils.aggregates import get_category_totals, get_daily_totals, get_transaction_summary,get_users_by_transaction_filters
from . import api_bp

@api_bp.route('/analytics/category-totals', methods=['GET'])
def category_totals():
    """Get transaction totals grouped by category"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    try:
        totals = get_category_totals(start_date, end_date)
        return jsonify({
            'status': 'success',
            'data': totals,
            'filters': {
                'start_date': start_date,
                'end_date': end_date
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400

@api_bp.route('/analytics/daily-totals', methods=['GET'])
def daily_totals():
    """Get daily transaction totals"""
    try:
        days = request.args.get('days', default=30, type=int)
        if days <= 0:
            raise ValueError('Days must be a positive number')
            
        totals = get_daily_totals(days)
        return jsonify({
            'status': 'success',
            'data': totals,
            'filters': {
                'days': days
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400

@api_bp.route('/analytics/summary', methods=['GET'])
def summary():
    """Get a summary of transaction data"""
    try:
        summary_data = get_transaction_summary()
        return jsonify({
            'status': 'success',
            'data': summary_data
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

# Add this new endpoint to analytics.py

@api_bp.route('/analytics/users', methods=['GET'])
def user_analytics():
    """Get users matching complex transaction filters with pagination"""
    try:
        # Parse query parameters for filtering
        min_transactions = request.args.get('min_transactions', default=1, type=int)
        min_amount = request.args.get('min_amount', type=float)
        city_tier = request.args.get('city_tier', type=int)
        transaction_type = request.args.get('transaction_type')
        category = request.args.get('category')
        days = request.args.get('days', type=int)

        # Parse query parameters for pagination
        page = request.args.get('page', default=1, type=int)
        per_page = request.args.get('per_page', default=20, type=int)

        # Get paginated results
        paginated_data = get_users_by_transaction_filters(
            min_transactions=min_transactions,
            min_amount=min_amount,
            city_tier=city_tier,
            transaction_type=transaction_type,
            category=category,
            days=days,
            page=page,
            per_page=per_page
        )

        return jsonify({
            'status': 'success',
            'data': paginated_data['items'],
            'pagination': {
                'total': paginated_data['total'],
                'pages': paginated_data['pages'],
                'page': paginated_data['page'],
                'per_page': per_page
            },
            'filters': {
                'min_transactions': min_transactions,
                'min_amount': min_amount,
                'city_tier': city_tier,
                'transaction_type': transaction_type,
                'category': category,
                'days': days
            }
        })

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400