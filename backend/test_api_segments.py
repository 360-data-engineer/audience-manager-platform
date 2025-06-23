# test_api_segments.py
import pytest
from app import create_app, db

def test_get_segment_by_rule(client):
    response = client.get('/api/v1/segments/by_rule/1')
    assert response.status_code == 200
    data = response.get_json()
    assert data['status'] == 'success'
    assert data['data']['rule_id'] == 1
    assert data['data']['table_name'] == 'segment_output_1'
    assert data['data']['row_count'] == 1

def test_get_segment_details(client):
    response = client.get('/api/v1/segments/2')
    assert response.status_code == 200
    data = response.get_json()
    assert data['status'] == 'success'
    assert data['data']['segment_name'] == 'segment_1'
    assert data['data']['row_count'] == 1
    assert isinstance(data['data']['sample_data'], list)
    assert data['data']['sample_data'][0]['transaction_types'] == 'UPI'

@pytest.fixture
def client(monkeypatch):
    # Patch init_scheduler to no-op to avoid SchedulerAlreadyRunningError
    monkeypatch.setattr('app.core.scheduler.init_scheduler', lambda app: None)
    app = create_app()
    app.config['TESTING'] = True
    with app.test_client() as client:
        with app.app_context():
            db.create_all()
            # Seed Rule and SegmentCatalog
            from app.models.rule_engine import Rule, SegmentCatalog
            import datetime
            rule = Rule(
                id=1,
                rule_name="high_value_customers",
                description="Test rule",
                conditions={},
                is_active=True,
                schedule="DAILY"
            )
            segment = SegmentCatalog(
                id=2,
                segment_name="segment_1",
                description="Segment for rule: high_value_customers",
                table_name="segment_output_1",
                row_count=1,
                rule_id=1,
                refresh_frequency="DAILY",
                last_refreshed_at=datetime.datetime.utcnow(),
                created_at=datetime.datetime.utcnow()
            )
            db.session.merge(rule)
            db.session.merge(segment)
            db.session.commit()
            # Drop segment_output_1 if it exists, then create
            db.session.execute("DROP TABLE IF EXISTS segment_output_1;")
            db.session.execute("""
                CREATE TABLE segment_output_1 (
                    user_id bigint,
                    total_transactions decimal(20,2),
                    total_spent decimal(20,2),
                    transaction_types string
                )
            """)
            db.session.execute("""
                INSERT INTO segment_output_1 (user_id, total_transactions, total_spent, transaction_types)
                VALUES (0, 26, 1383106.15, 'UPI')
            """)
            db.session.commit()
        yield client
        with app.app_context():
            db.session.remove()
            db.drop_all()
