# app/__init__.py
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from flask_migrate import Migrate
from config import config
import os
IS_MIGRATING = os.environ.get('IS_MIGRATING') == '1'

# Initialize extensions without importing models
db = SQLAlchemy()
cors = CORS()
migrate = Migrate()

def create_app(config_name='default'):
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    
    # Initialize extensions
    db.init_app(app)
    cors.init_app(app)
    migrate.init_app(app, db)
    
    try:
        # Import models before creating tables
        from .models import transactions, rule_engine
        from .api import api_bp
        from .core.scheduler import init_scheduler
        
        # Ensure tables exist after models are imported
        with app.app_context():
            db.create_all()
        
        # Register blueprints
        app.register_blueprint(api_bp, url_prefix='/api/v1')
        
        # Initialize scheduler in non-testing environments
        if not app.config.get('TESTING') and not IS_MIGRATING:
            try:
                init_scheduler(app)
            except Exception as e:
                app.logger.error(f"Failed to initialize scheduler: {str(e)}")
                app.logger.info("Continuing without scheduler...")
        
        # Root endpoint
        @app.route('/')
        def index():
            return jsonify({
                'name': 'Audience Manager API',
                'version': '1.0.0',
                'endpoints': {
                    'health': '/api/v1/health',
                    'transactions': {
                        'list_upi': '/api/v1/transactions/upi',
                        'list_credit_card': '/api/v1/transactions/credit-card'
                    },
                    'analytics': {
                        'category_totals': '/api/v1/analytics/category-totals',
                        'daily_totals': '/api/v1/analytics/daily-totals',
                        'summary': '/api/v1/analytics/summary',
                        'users': '/api/v1/analytics/users'
                    },
                    'segments': {
                        'list': '/api/v1/segments',
                        'by_rule': '/api/v1/segments/rule/<int:rule_id>',
                        'detail': '/api/v1/segments/<int:segment_id>',
                        'refresh': '/api/v1/segments/<int:segment_id>/refresh'
                    },
                    'rules': {
                        'list': '/api/v1/rules',
                        'detail': '/api/v1/rules/<int:rule_id>',
                        'trigger': '/api/v1/rules/<int:rule_id>/trigger'
                    }
                }
            })
        
        return app
        
    except Exception as e:
        app.logger.error(f"Failed to initialize application: {str(e)}")
        raise