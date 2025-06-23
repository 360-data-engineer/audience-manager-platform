from datetime import datetime, timedelta
from sqlalchemy import func, extract, and_
from ..models import UPITransaction, CreditCardPayment

def get_category_totals(start_date=None, end_date=None):
    """Get transaction totals grouped by category"""
    # Base queries
    upi_query = UPITransaction.query
    cc_query = CreditCardPayment.query
    
    # Apply date filters if provided
    if start_date:
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        upi_query = upi_query.filter(UPITransaction.transaction_date >= start_date)
        cc_query = cc_query.filter(CreditCardPayment.transaction_date >= start_date)
    if end_date:
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        upi_query = upi_query.filter(UPITransaction.transaction_date <= end_date)
        cc_query = cc_query.filter(CreditCardPayment.transaction_date <= end_date)
    
    # Get UPI totals by category
    upi_totals = upi_query.with_entities(
        UPITransaction.category,
        func.count(UPITransaction.id).label('count'),
        func.sum(UPITransaction.amount).label('total_amount')
    ).group_by(UPITransaction.category).all()
    
    # Get Credit Card totals by category
    cc_totals = cc_query.with_entities(
        CreditCardPayment.category,
        func.count(CreditCardPayment.id).label('count'),
        func.sum(CreditCardPayment.amount).label('total_amount')
    ).group_by(CreditCardPayment.category).all()
    
    # Combine and format results
    result = {}
    for category, count, total in upi_totals:
        if not category:  # Skip None categories
            continue
        if category not in result:
            result[category] = {'count': 0, 'total_amount': 0, 'type': 'combined'}
        result[category]['count'] += count or 0
        result[category]['total_amount'] += float(total or 0)
    
    for category, count, total in cc_totals:
        if not category:  # Skip None categories
            continue
        if category not in result:
            result[category] = {'count': 0, 'total_amount': 0, 'type': 'combined'}
        result[category]['count'] += count or 0
        result[category]['total_amount'] += float(total or 0)
    
    return result

def get_daily_totals(days=30):
    """Get daily transaction totals for the last N days"""
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)
    
    # Format dates for SQL query
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    # Get UPI daily totals
    upi_daily = UPITransaction.query.with_entities(
        func.date(UPITransaction.transaction_date).label('date'),
        func.count(UPITransaction.id).label('count'),
        func.sum(UPITransaction.amount).label('total_amount')
    ).filter(
        and_(
            func.date(UPITransaction.transaction_date) >= start_date_str,
            func.date(UPITransaction.transaction_date) <= end_date_str
        )
    ).group_by('date').all()
    
    # Get Credit Card daily totals
    cc_daily = CreditCardPayment.query.with_entities(
        func.date(CreditCardPayment.transaction_date).label('date'),
        func.count(CreditCardPayment.id).label('count'),
        func.sum(CreditCardPayment.amount).label('total_amount')
    ).filter(
        and_(
            func.date(CreditCardPayment.transaction_date) >= start_date_str,
            func.date(CreditCardPayment.transaction_date) <= end_date_str
        )
    ).group_by('date').all()
    
    # Initialize result with all dates in range
    result = {}
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        result[date_str] = {'count': 0, 'total_amount': 0}
        current_date += timedelta(days=1)
    
    # Update with actual data
    for date, count, total in upi_daily + cc_daily:
        if date:  # Skip None dates
            date_str = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)[:10]
            if date_str in result:
                result[date_str]['count'] += count or 0
                result[date_str]['total_amount'] = round(float((result[date_str]['total_amount'] or 0) + (total or 0)), 2)
    
    return result

def get_transaction_summary():
    """Get a summary of transaction data"""
    # Get category totals
    category_data = get_category_totals()
    
    # Get daily totals for the last 30 days
    daily_data = get_daily_totals(30)
    
    # Calculate total transactions and amount
    total_transactions = sum(data['count'] for data in category_data.values())
    total_amount = round(sum(data['total_amount'] for data in category_data.values()), 2)
    
    # Get top categories
    top_categories = sorted(
        [{'category': k, **v} for k, v in category_data.items() if k],  # Filter out None categories
        key=lambda x: x['total_amount'],
        reverse=True
    )[:5]
    
    return {
        'total_transactions': total_transactions,
        'total_amount': total_amount,
        'top_categories': top_categories,
        'daily_totals': daily_data
    }

# Add these new functions to aggregates.py

def get_users_by_transaction_filters(
    min_transactions=1,
    min_amount=None,
    city_tier=None,
    transaction_type=None,
    category=None,
    days=None,
    page=1,
    per_page=20
):
    """
    Get users matching complex transaction filters with pagination.
    """
    from sqlalchemy import union_all, select

    # Define common columns for union
    upi_q = UPITransaction.query.with_entities(
        UPITransaction.user_id,
        UPITransaction.amount,
        UPITransaction.city_tier,
        UPITransaction.category,
        UPITransaction.transaction_date
    )
    cc_q = CreditCardPayment.query.with_entities(
        CreditCardPayment.user_id,
        CreditCardPayment.amount,
        CreditCardPayment.city_tier,
        CreditCardPayment.category,
        CreditCardPayment.transaction_date
    )

    # Apply filters based on transaction type
    if transaction_type == 'upi':
        queries = [upi_q]
    elif transaction_type == 'credit_card':
        queries = [cc_q]
    else:
        queries = [upi_q, cc_q]

    # Apply common filters to each query
    filtered_queries = []
    for q in queries:
        if min_amount is not None:
            q = q.filter(UPITransaction.amount >= min_amount)
        if city_tier is not None:
            q = q.filter(UPITransaction.city_tier == city_tier)
        if category is not None:
            q = q.filter(UPITransaction.category == category)
        if days is not None:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            q = q.filter(UPITransaction.transaction_date >= cutoff_date)
        filtered_queries.append(q)

    # Combine queries using UNION ALL
    if not filtered_queries:
        return {'total': 0, 'items': [], 'pages': 0, 'page': page}

    combined_query = union_all(*[q.statement for q in filtered_queries]).alias('transactions')

    # Build the final aggregation query
    final_query = select([
        combined_query.c.user_id,
        func.count(combined_query.c.user_id).label('transaction_count'),
        func.sum(combined_query.c.amount).label('total_amount')
    ]).group_by(combined_query.c.user_id).having(
        func.count(combined_query.c.user_id) >= min_transactions
    )

    # Get total count for pagination
    from ..utils.database import db
    total_count_query = select([func.count()]).select_from(final_query.alias('sub'))
    total = db.session.execute(total_count_query).scalar_one_or_none() or 0

    # Apply pagination
    paginated_query = final_query.limit(per_page).offset((page - 1) * per_page)
    results = db.session.execute(paginated_query).fetchall()

    # Format results
    items = [{
        'user_id': r.user_id,
        'transaction_count': r.transaction_count,
        'total_amount': round(float(r.total_amount or 0), 2)
    } for r in results]

    return {
        'total': total,
        'items': items,
        'pages': (total + per_page - 1) // per_page,
        'page': page
    }
