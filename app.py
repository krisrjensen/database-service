#!/usr/bin/env python3
"""
Database Service
Flask REST API for V3 arc detection database
20250602_100100_0_0_1_1
"""

from flask import Flask, request, jsonify
import sqlite3
import numpy as np
import sys
from pathlib import Path
from api.files import files_api
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

DATABASE_PATH = "/Volumes/ArcData/V3_database/arc_detection.db"
BINARY_DATA_DIR = "/Volumes/ArcData/V3_database/fileset"

app = Flask(__name__)

# Configure rate limiting
limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["1000 per hour", "100 per minute"]
)

# Register API blueprints
app.register_blueprint(files_api)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'database',
        'version': '20250602_100100_0_0_1_1'
    })

@app.route('/', methods=['GET'])
def index():
    """Root endpoint"""
    return jsonify({
        'service': 'Database Service',
        'version': '20250602_100100_0_0_1_1',
        'endpoints': {
            'files': '/api/files',
            'file_data': '/api/files/{id}/data',
            'file_details': '/api/files/{id}',
            'update_label': '/api/files/{id}/label',
            'update_transients': '/api/files/{id}/transients',
            'search': '/api/search',
            'statistics': '/api/labels/statistics',
            'status': '/api/status',
            'health': '/health'
        }
    })

if __name__ == "__main__":
    print("Starting Database Service...")
    print("Available at: http://localhost:5001")
    app.run(host='0.0.0.0', port=5001, debug=True)