# Update app/api/rules.py
import json
from flask import Blueprint, request, jsonify, current_app
from datetime import datetime
from ..models.rule_engine import Rule, SegmentCatalog, db
from ..core.scheduler import schedule_rule, execute_rule, remove_scheduled_rule, scheduler
from ..utils.rule_parser import RuleParser
from ..utils.dependency_finder import find_best_dependency
from . import api_bp

@api_bp.route('/rules', methods=['POST'])
def create_rule():
    """Create a new segmentation rule with automatic dependency detection"""
    try:
        data = request.get_json()
        
        required_fields = ['rule_name', 'conditions']
        if not all(field in data for field in required_fields):
            return jsonify({'status': 'error', 'message': 'Missing required fields: rule_name, conditions'}), 400

        if Rule.query.filter_by(rule_name=data['rule_name']).first():
            return jsonify({'status': 'error', 'message': f"A rule with the name '{data['rule_name']}' already exists."}), 409

        conditions = data.get('conditions', [])
        
        # Automatically find the best dependency
        dependency_info = find_best_dependency(conditions)
        
        dependencies = None
        operation = None
        rule_conditions = conditions

        if dependency_info:
            dependencies, operation, rule_conditions = dependency_info
            current_app.logger.info(f"Rule '{data['rule_name']}' will depend on rule(s) {dependencies} with remaining conditions.")
        else:
            current_app.logger.info(f"No suitable dependency found for rule '{data['rule_name']}'. Creating as a base rule.")

        # Create the rule with potentially modified conditions
        rule = Rule(
            rule_name=data['rule_name'],
            description=data.get('description', ''),
            conditions=rule_conditions,
            dependencies=dependencies,
            operation=operation,
            is_active=data.get('is_active', True),
            schedule=data.get('schedule', 'once'),
            next_run_at=datetime.utcnow()
        )
        rule.save()

        # Generate SQL for the segment based on its own (potentially remaining) conditions
        sql_query = RuleParser.generate_segment_sql(rule.id, rule.conditions)

        # Create segment catalog entry
        segment = SegmentCatalog(
            segment_name=f"segment_{rule.id}",
            description=f"Segment for rule: {rule.rule_name}",
            table_name=f"segment_output_{rule.id}",
            rule_id=rule.id,
            sql_query=sql_query,
            depends_on=json.dumps(dependencies) if dependencies else None,
            operation=operation,
            refresh_frequency=rule.schedule
        )
        segment.save()

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
        current_app.logger.error(f"Error creating rule: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

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
            rule = Rule.query.get_or_404(rule_id)
            data = request.get_json()
            
            conditions = data.get('conditions', rule.conditions)

            # Automatically find the best dependency, excluding the current rule
            dependency_info = find_best_dependency(conditions, rule_id_to_exclude=rule_id)

            dependencies = None
            operation = None
            rule_conditions = conditions

            if dependency_info:
                dependencies, operation, rule_conditions = dependency_info
                current_app.logger.info(f"Updating Rule {rule_id}. Found dependency: {dependencies}")
            else:
                current_app.logger.info(f"Updating Rule {rule_id}. No suitable dependency found.")

            # Update rule fields
            rule.rule_name = data.get('rule_name', rule.rule_name)
            rule.description = data.get('description', rule.description)
            rule.conditions = rule_conditions
            rule.dependencies = dependencies
            rule.operation = operation
            rule.is_active = data.get('is_active', rule.is_active)
            rule.schedule = data.get('schedule', rule.schedule)
            rule.save()

            # Update associated segment catalog
            segment = SegmentCatalog.query.filter_by(rule_id=rule.id).first()
            if segment:
                sql_query = RuleParser.generate_segment_sql(rule.id, rule.conditions)
                
                segment.sql_query = sql_query
                segment.depends_on = json.dumps(dependencies) if dependencies else None
                segment.operation = operation
                segment.refresh_frequency = rule.schedule

            # Update dependency fields
            rule.dependencies = data.get('dependencies', rule.dependencies)
            rule.operation = data.get('operation', rule.operation)

            rule.updated_at = datetime.utcnow()
            rule.save()

            # Update the corresponding segment catalog entry
            segment = SegmentCatalog.query.filter_by(rule_id=rule_id).first()
            if segment:
                segment.description = f"Segment for rule: {rule.rule_name}"
                segment.refresh_frequency = rule.schedule
                segment.depends_on = json.dumps(rule.dependencies) if rule.dependencies else None
                segment.operation = rule.operation

                # Regenerate SQL based on updated conditions/dependencies
                if rule.dependencies:
                    segment.sql_query = f"COMPOUND_OPERATION:{rule.operation}"
                else:
                    segment.sql_query = RuleParser.generate_segment_sql(rule.id, rule.conditions)
                
                segment.save()

            return jsonify({
                'status': 'success',
                'data': rule.to_dict()
            })

        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f"Error updating rule {rule_id}: {e}", exc_info=True)
            return jsonify({'status': 'error', 'message': str(e)}), 500

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