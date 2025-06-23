# Update app/api/rules.py
from flask import Blueprint, request, jsonify, current_app
from datetime import datetime
from ..models.rule_engine import Rule, SegmentCatalog, db
from ..core.scheduler import schedule_rule, execute_rule, remove_scheduled_rule, scheduler
from ..utils.rule_parser import RuleParser
from . import api_bp

@api_bp.route('/rules', methods=['POST'])
def create_rule():
    """Create a new segmentation rule"""
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['rule_name', 'conditions']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'status': 'error',
                    'message': f'Missing required field: {field}'
                }), 400
        
        # Create the rule
        rule = Rule(
            rule_name=data['rule_name'],
            description=data.get('description', ''),
            conditions=data['conditions'],
            schedule=data.get('schedule', 'once'),
            is_active=data.get('is_active', True),
            next_run_at=datetime.utcnow()  # Schedule first run immediately
        ).save()
        
        # Generate SQL for the segment
        sql_query = RuleParser.generate_segment_sql(rule.id, rule.conditions)

        # Create segment catalog entry
        segment = SegmentCatalog(
            segment_name=f"segment_{rule.id}",
            description=f"Segment for rule: {rule.rule_name}",
            table_name=f"segment_output_{rule.id}",
            rule_id=rule.id,
            refresh_frequency=rule.schedule,
            sql_query=sql_query
        ).save()
        
        # Schedule the job
        schedule_rule(rule.id)
        
        return jsonify({
            'status': 'success',
            'data': {
                'rule': rule.to_dict(),
                'segment': segment.to_dict()
            }
        }), 201
        
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400

@api_bp.route('/rules', methods=['GET'])
def list_rules():
    """List all rules with pagination"""
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 10, type=int)
        
        rules = Rule.query.paginate(page=page, per_page=per_page, error_out=False)
        
        return jsonify({
            'status': 'success',
            'data': {
                'items': [rule.to_dict() for rule in rules.items],
                'total': rules.total,
                'pages': rules.pages,
                'current_page': rules.page
            }
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400

@api_bp.route('/rules/<int:rule_id>', methods=['GET', 'PUT', 'DELETE'])
def manage_rule(rule_id):
    """
    GET: Get rule details by ID
    PUT: Update an existing rule
    DELETE: Delete a rule
    """
    if request.method == 'GET':
        try:
            rule = Rule.query.get_or_404(rule_id)
            return jsonify({
                'status': 'success',
                'data': rule.to_dict()
            })
        except Exception as e:
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 404
    
    elif request.method == 'DELETE':
        try:
            rule = Rule.query.get_or_404(rule_id)

            # 1. Remove from scheduler
            remove_scheduled_rule(rule.id)

            # 2. Delete associated segment catalog
            SegmentCatalog.query.filter_by(rule_id=rule.id).delete()

            # 3. Delete the rule
            db.session.delete(rule)
            db.session.commit()

            return jsonify({
                'status': 'success',
                'message': f'Rule {rule_id} and associated data deleted successfully.'
            }), 200

        except Exception as e:
            db.session.rollback()
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500
    
    elif request.method == 'PUT':
        try:
            data = request.get_json()
            rule = Rule.query.get_or_404(rule_id)
            
            # Update fields if they exist in the request
            if 'description' in data:
                rule.description = data['description']
            if 'is_active' in data:
                rule.is_active = data['is_active']
            if 'conditions' in data:
                rule.conditions = data['conditions']
            if 'schedule' in data:
                rule.schedule = data['schedule']
            
            rule.updated_at = datetime.utcnow()
            db.session.commit()
            
            # Also update the corresponding segment catalog entry if it exists
            segment = SegmentCatalog.query.filter_by(rule_id=rule_id).first()
            if segment:
                segment.description = f"Segment for rule: {rule.rule_name}"
                segment.refresh_frequency = rule.schedule
                
                # Regenerate SQL if conditions changed
                if 'conditions' in data:
                    segment.sql_query = RuleParser.generate_segment_sql(rule.id, rule.conditions)
                
                db.session.commit()
            
            return jsonify({
                'status': 'success',
                'data': rule.to_dict()
            })
            
        except Exception as e:
            db.session.rollback()
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 400

@api_bp.route('/rules/<int:rule_id>/trigger', methods=['POST'])
def trigger_rule(rule_id):
    """Manually trigger a rule execution by queueing it with the scheduler"""
    try:
        # Ensure the rule exists
        Rule.query.get_or_404(rule_id)
        
        # Get scheduler from the scheduler module and add a one-off job
        if not scheduler or not scheduler.running:
            return jsonify({
                'status': 'error',
                'message': 'Scheduler is not running.'
            }), 500
        job_id = f'manual_run_{rule_id}_{int(datetime.utcnow().timestamp())}'
        
        scheduler.add_job(
            func=execute_rule,
            args=[rule_id],
            id=job_id,
            trigger='date',
            run_date=datetime.utcnow(),
            replace_existing=False, # Avoid replacing other jobs
            misfire_grace_time=None # Execute immediately if missed
        )
        
        return jsonify({
            'status': 'success',
            'message': f'Rule {rule_id} execution has been queued.',
            'data': { 'rule_id': rule_id }
        })
    except Exception as e:
        current_app.logger.error(f"Failed to trigger rule {rule_id}: {e}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Failed to queue rule execution: {e}'
        }), 500