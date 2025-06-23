from . import api_bp

@api_bp.route('/health', methods=['GET'])
def health_check():
    """Basic health check endpoint."""
    return {'status': 'healthy'}, 200
