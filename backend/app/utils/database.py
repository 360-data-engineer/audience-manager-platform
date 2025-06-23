from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager
from flask import current_app

class DatabaseManager:
    def __init__(self, db=None):
        """Initialize with an optional SQLAlchemy instance."""
        self.db = db
        self._engine = None
        self._session_factory = None
        self._app = None
    
    def init_app(self, app):
        """Initialize the database with the given Flask app."""
        self._app = app
        # We'll create the engine and session factory lazily when first needed
        app.teardown_appcontext(self.close_session)
    
    def _get_engine(self):
        """Get or create the database engine."""
        if self._engine is None:
            if self.db is not None and hasattr(self.db, 'engine'):
                self._engine = self.db.engine
            elif self._app is not None:
                # Create engine directly from app config
                database_uri = self._app.config.get('SQLALCHEMY_DATABASE_URI')
                self._engine = create_engine(database_uri)
            else:
                raise RuntimeError("Neither SQLAlchemy instance nor app is configured")
        return self._engine
    
    @property
    def engine(self):
        """Get the database engine."""
        return self._get_engine()
    
    @property
    def Session(self):
        """Get the scoped session."""
        if not hasattr(self, '_session_factory') or self._session_factory is None:
            self._session_factory = scoped_session(
                sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            )
        return self._session_factory
    
    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
    
    def get_session(self):
        """Get a new database session."""
        return self.Session()
    
    def close_session(self, exception=None):
        """Close the current database session."""
        if hasattr(self, 'Session') and self.Session:
            self.Session.remove()

# This will be initialized in app/__init__.py after db is created
db_manager = DatabaseManager()