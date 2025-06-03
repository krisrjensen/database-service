"""
REST API for database file operations
20250602_101700_0_0_1_5
"""

from flask import Flask, request, jsonify, Blueprint
import sqlite3
import os
import logging
from functools import wraps
from ..database.operations import V3Database

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

files_api = Blueprint('files_api', __name__)
db = V3Database()

def handle_errors(f):
    """Decorator for comprehensive error handling"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except ValueError as e:
            logger.error(f"Validation error in {f.__name__}: {str(e)}")
            return jsonify({
                'status': 'error',
                'error_type': 'validation_error',
                'message': str(e)
            }), 400
        except sqlite3.Error as e:
            logger.error(f"Database error in {f.__name__}: {str(e)}")
            return jsonify({
                'status': 'error',
                'error_type': 'database_error',
                'message': 'Database operation failed'
            }), 500
        except FileNotFoundError as e:
            logger.error(f"File not found in {f.__name__}: {str(e)}")
            return jsonify({
                'status': 'error',
                'error_type': 'file_not_found',
                'message': 'Requested file not found'
            }), 404
        except Exception as e:
            logger.error(f"Unexpected error in {f.__name__}: {str(e)}")
            return jsonify({
                'status': 'error',
                'error_type': 'internal_error',
                'message': 'Internal server error'
            }), 500
    return decorated_function

def validate_file_id(file_id):
    """Validate file ID parameter"""
    if not isinstance(file_id, int) or file_id <= 0:
        raise ValueError("File ID must be a positive integer")
    return file_id

def validate_json_request(required_fields=None):
    """Validate JSON request data"""
    if not request.is_json:
        raise ValueError("Request must be JSON")
    
    data = request.get_json()
    if not data:
        raise ValueError("Request body cannot be empty")
    
    if required_fields:
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
    
    return data

@files_api.route('/api/files', methods=['GET'])
@handle_errors
def get_files():
    """Get list of files from database"""
    label_filter = request.args.get('label')
    if label_filter:
        # Validate label filter
        valid_labels = ['arc', 'weak_arc', 'restriking_arc', 'parallel_motor_arc', 
                       'negative_transient', 'steady_state', 'other']
        if label_filter not in valid_labels:
            raise ValueError(f"Invalid label filter. Must be one of: {', '.join(valid_labels)}")
    
    files_data = db.get_all_files(label_filter=label_filter)
    
    files = []
    for file_row in files_data:
        files.append({
            'file_id': file_row[0],
            'filename': file_row[1],
            'path': file_row[2],
            'label': file_row[3],
            'transient1_index': file_row[4],
            'transient2_index': file_row[5],
            'transient3_index': file_row[6],
            'voltage_level': file_row[7],
            'current_level': file_row[8],
            'binary_path': file_row[9]
        })
    
    return jsonify({
        'status': 'success',
        'files': files,
        'count': len(files)
    })

@files_api.route('/api/files/<int:file_id>', methods=['GET'])
@handle_errors
def get_file(file_id):
    """Get specific file by ID"""
    validate_file_id(file_id)
    
    file_data = db.get_file_by_id(file_id)
    if not file_data:
        raise FileNotFoundError(f"File with ID {file_id} not found")
    
    file_info = {
        'file_id': file_data[0],
        'filename': file_data[1],
        'path': file_data[2],
        'label': file_data[3],
        'transient1_index': file_data[4],
        'transient2_index': file_data[5],
        'transient3_index': file_data[6],
        'voltage_level': file_data[7],
        'current_level': file_data[8],
        'binary_path': file_data[9],
        'total_samples': file_data[10] if len(file_data) > 10 else None,
        'sampling_rate': file_data[11] if len(file_data) > 11 else None
    }
    
    return jsonify({
        'status': 'success',
        'file': file_info
    })

@files_api.route('/api/files/<int:file_id>/data', methods=['GET'])
def get_file_data(file_id):
    """Get signal data for specific file"""
    try:
        load_voltage, source_current = db.load_file_data(file_id)
        if load_voltage is None:
            return jsonify({
                'status': 'error',
                'message': 'File data not found'
            }), 404
        
        return jsonify({
            'status': 'success',
            'data': {
                'load_voltage': load_voltage.tolist() if load_voltage is not None else None,
                'source_current': source_current.tolist() if source_current is not None else None
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@files_api.route('/api/files/<int:file_id>/label', methods=['PUT'])
def update_file_label(file_id):
    """Update file label"""
    try:
        data = request.get_json()
        if not data or 'label' not in data:
            return jsonify({
                'status': 'error',
                'message': 'Missing label in request'
            }), 400
        
        db.update_file_label(file_id, data['label'])
        
        return jsonify({
            'status': 'success',
            'message': f'Updated file {file_id} label to {data["label"]}'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@files_api.route('/api/files/<int:file_id>/transients', methods=['PUT'])
def update_transients(file_id):
    """Update transient indices"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'status': 'error',
                'message': 'Missing request data'
            }), 400
        
        db.update_transient_indices(
            file_id,
            data.get('transient1'),
            data.get('transient2'),
            data.get('transient3')
        )
        
        return jsonify({
            'status': 'success',
            'message': f'Updated transient indices for file {file_id}'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@files_api.route('/api/status', methods=['GET'])
def get_status_summary():
    """Get status summary"""
    try:
        summary = db.get_status_summary()
        return jsonify({
            'status': 'success',
            'summary': summary
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@files_api.route('/api/labels/statistics', methods=['GET'])
def get_label_statistics():
    """Get label statistics"""
    try:
        stats = db.get_label_statistics()
        return jsonify({
            'status': 'success',
            'statistics': [{'label': label, 'count': count} for label, count in stats]
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@files_api.route('/api/search', methods=['GET'])
def search_files():
    """Search files by criteria"""
    try:
        filename = request.args.get('filename')
        voltage_min = request.args.get('voltage_min', type=float)
        voltage_max = request.args.get('voltage_max', type=float)
        current_min = request.args.get('current_min', type=float)
        current_max = request.args.get('current_max', type=float)
        
        voltage_range = None
        if voltage_min is not None and voltage_max is not None:
            voltage_range = (voltage_min, voltage_max)
        
        current_range = None
        if current_min is not None and current_max is not None:
            current_range = (current_min, current_max)
        
        results = db.search_files(filename, voltage_range, current_range)
        
        return jsonify({
            'status': 'success',
            'results': [{'file_id': r[0], 'filename': r[1], 'label': r[2], 
                        'voltage': r[3], 'current': r[4]} for r in results]
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500