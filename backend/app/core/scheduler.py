from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import logging
from app.processor.spark_processor import SparkSegmentProcessor
import subprocess
import os
from pytz import utc

# Configure logger
logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(timezone=utc)

def init_scheduler(app):
    """Initialize the scheduler with the app context"""
    if scheduler.running:
        logger.info("Scheduler is already running. Skipping re-initialization.")
        return

    scheduler.app = app
    scheduler.start()
    
    # Import here to avoid circular imports
    from ..models.rule_engine import Rule
    
    # Schedule all active rules
    with app.app_context():
        rules = Rule.query.filter_by(is_active=True).all()
        for rule in rules:
            if not rule.next_run_at or rule.next_run_at < datetime.utcnow():
                rule.next_run_at = datetime.utcnow()
                rule.save()
            schedule_rule(rule.id)

def remove_scheduled_rule(rule_id: int):
    """Remove a scheduled job for a given rule ID."""
    job_id = f'rule_{rule_id}'
    try:
        scheduler.remove_job(job_id)
        logger.info(f"Removed job {job_id} from scheduler.")
    except Exception as e:
        # It's possible the job doesn't exist, which is fine.
        logger.warning(f"Could not remove job {job_id}. It might not exist. Error: {e}")

def schedule_rule(rule_id: int):
    """Schedule a rule to run at its next scheduled time"""
    from ..models.rule_engine import Rule
    
    rule = Rule.query.get(rule_id)
    if not rule or not rule.is_active:
        return
    
    scheduler.add_job(
        execute_rule,
        'date',
        run_date=rule.next_run_at,
        args=[rule.id],
        id=f'rule_{rule.id}',
        replace_existing=True
    )

def execute_rule(rule_id: int):
    """The function that gets executed by the scheduler."""
    from app import create_app, db
    from app.models import Rule

    app = create_app()
    with app.app_context():
        try:
            logger.info(f"Executing rule {rule_id}")

            # --- START: Dynamic Spark Path Logic ---
            spark_home_env = os.environ.get('SPARK_HOME')
            possible_paths = [
                os.path.join(spark_home_env, 'bin', 'spark-submit') if spark_home_env else None,
                '/opt/spark-3.5/bin/spark-submit',
                '/opt/homebrew/Cellar/apache-spark/4.0.0/libexec/bin/spark-submit',
                '/opt/homebrew/bin/spark-submit',
            ]

            spark_submit_cmd = None
            for path in possible_paths:
                if path and os.path.isfile(path) and os.access(path, os.X_OK):
                    spark_submit_cmd = path
                    logger.info(f"Found spark-submit executable at: {spark_submit_cmd}")
                    break
            
            if not spark_submit_cmd:
                logger.error("Could not find a valid spark-submit executable. Searched paths: " + str([p for p in possible_paths if p]))
                logger.error("Please ensure SPARK_HOME is set correctly or Spark is in a standard location.")
                return False
            # --- END: Dynamic Spark Path Logic ---

            # Construct absolute path to the Spark job script
            app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
            spark_job_path = os.path.join(app_dir, 'jobs', 'segment_processor_job.py')

            # Construct absolute path to the JAR file
            backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
            jar_path = os.path.join(backend_dir, 'jars', 'sqlite-jdbc-3.45.3.0.jar')

            logger.info(f"Submitting Spark job with JAR: {jar_path}")

            db_uri = app.config.get('SQLALCHEMY_DATABASE_URI')
            if not db_uri:
                logger.error("SQLALCHEMY_DATABASE_URI is not configured in the Flask app.")
                return False

            cmd = [
                spark_submit_cmd,
                '--master', 'local[*]',
                '--jars', jar_path,
                '--driver-class-path', jar_path,
                spark_job_path,
                str(rule_id),
                db_uri
            ]

            logger.debug(f"Running command: {' '.join(cmd)}")

            # Set PYTHONPATH for the Spark job to find the 'app' module
            env = os.environ.copy()
            python_path = env.get('PYTHONPATH', '')
            # The project root is 'backend_dir', which is two levels up from this file's directory.
            env['PYTHONPATH'] = f"{backend_dir}:{python_path}" if python_path else backend_dir
            logger.debug(f"Using PYTHONPATH for Spark job: {env['PYTHONPATH']}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)

            # Always log stdout and stderr for debugging purposes
            if result.stdout:
                logger.info(f"Spark Job STDOUT for rule {rule_id}:\n{result.stdout}")
            if result.stderr:
                # Spark often logs informational messages to stderr, so we log it as a warning.
                logger.warning(f"Spark Job STDERR for rule {rule_id}:\n{result.stderr}")

            if result.returncode == 0:
                logger.info(f"Spark job for rule {rule_id} finished successfully.")
                
                rule = Rule.query.get(rule_id)
                if rule:
                    rule.last_run_at = datetime.utcnow()
                    db.session.commit()
                    logger.info(f"Successfully executed and updated last_run_at for rule {rule_id}")
                return True
            else:
                logger.error(f"Spark job for rule {rule_id} failed with code {result.returncode}")
                return False

        except Exception as e:
            logger.error(f"An exception occurred while executing rule {rule_id}: {e}", exc_info=True)
            return False

def calculate_next_run(schedule: str) -> datetime:
    """Calculate next run time based on schedule"""
    now = datetime.utcnow()
    if schedule == 'HOURLY':
        return now + timedelta(hours=1)
    elif schedule == 'DAILY':
        return now + timedelta(days=1)
    elif schedule == 'WEEKLY':
        return now + timedelta(weeks=1)
    else:  # Default to daily
        return now + timedelta(days=1)