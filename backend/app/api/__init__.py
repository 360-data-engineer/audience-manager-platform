# app/api/__init__.py
from flask import Blueprint

# Create API blueprint
api_bp = Blueprint('api', __name__)

# Import routes to register them with the blueprint
from . import routes, transactions, analytics, rules, segments  # noqa

# This makes the blueprint available for import
__all__ = ['api_bp']