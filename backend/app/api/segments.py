# app/api/segments.py
from flask import request, jsonify
from datetime import datetime
from ..models.rule_engine import SegmentCatalog, db
from . import api_bp

@api_bp.route('/segments', methods=['GET'])
def list_segments():
    """List all segments with pagination"""
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 10, type=int)
        
        segments = SegmentCatalog.query.paginate(page=page, per_page=per_page, error_out=False)
        
        return jsonify({
            'status': 'success',
            'data': {
                'items': [segment.to_dict() for segment in segments.items],
                'total': segments.total,
                'pages': segments.pages,
                'current_page': segments.page
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400

@api_bp.route('/segments/<int:segment_id>', methods=['GET'])
def get_segment(segment_id):
    """Get segment details by ID"""
    try:
        segment = SegmentCatalog.query.get_or_404(segment_id)
        return jsonify({
            'status': 'success',
            'data': segment.to_dict()
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 404

@api_bp.route('/segments/by_rule/<int:rule_id>', methods=['GET'])
def get_segment_by_rule(rule_id):
    """Get segment by rule ID"""
    try:
        segment = SegmentCatalog.query.filter_by(rule_id=rule_id).first_or_404()
        return jsonify({
            'status': 'success',
            'data': segment.to_dict()
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 404

@api_bp.route('/segments/<int:segment_id>/refresh', methods=['POST'])
def refresh_segment(segment_id):
    """Manually refresh a segment"""
    try:
        segment = SegmentCatalog.query.get_or_404(segment_id)
        
        # Update the last_refreshed_at timestamp
        segment.last_refreshed_at = datetime.utcnow()
        db.session.commit()
        
        return jsonify({
            'status': 'success',
            'message': f'Segment {segment_id} refresh triggered',
            'data': segment.to_dict()
        })
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400

@api_bp.route('/segments/<int:segment_id>/sample_data', methods=['GET'])
def get_segment_sample_data(segment_id):
    """Get sample data for a segment"""
    try:
        segment = SegmentCatalog.query.get_or_404(segment_id)
        
        sample_data = []
        # Ensure the table name is safe before using it in a raw query
        if segment.table_name and segment.table_name.isidentifier():
            try:
                from sqlalchemy import text
                # Use text() to safely handle parameters if needed, though not strictly necessary here
                query = text(f'SELECT * FROM "{segment.table_name}" LIMIT 10')
                result = db.session.execute(query)
                
                if result.returns_rows:
                    columns = result.keys()
                    sample_data = [dict(zip(columns, row)) for row in result.fetchall()]
            except Exception as e:
                # Log the error for debugging, but return empty list to frontend
                print(f"Error fetching sample data for {segment.table_name}: {e}")
                sample_data = []
        
        return jsonify({
            'status': 'success',
            'data': {
                'sample_data': sample_data
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 404
