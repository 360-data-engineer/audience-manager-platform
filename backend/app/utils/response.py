from flask import jsonify

def paginated_response(query, schema=None, page=1, per_page=20):
    """Generate a paginated response for a SQLAlchemy query."""
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    items = pagination.items
    
    if schema:
        items = [schema.dump(item) for item in items]
    else:
        items = [item.to_dict() for item in items]
    
    return {
        'items': items,
        'total': pagination.total,
        'pages': pagination.pages,
        'current_page': page,
        'per_page': per_page
    }

def success_response(data, status_code=200):
    """Return a success response with the given data."""
    return jsonify({
        'status': 'success',
        'data': data
    }), status_code

def error_response(message, status_code=400):
    """Return an error response with the given message."""
    return jsonify({
        'status': 'error',
        'message': message
    }), status_code