#!/usr/bin/env python3
"""
V3 Database Utilities
Provides database access and management functions for arc detection tools
"""

import sqlite3
import numpy as np
import os
import hashlib
from pathlib import Path
import time
import threading
from queue import Queue, Empty
from contextlib import contextmanager

# Configuration
DATABASE_PATH = "/Volumes/ArcData/V3_database/arc_detection.db"
BINARY_DATA_DIR = "/Volumes/ArcData/V3_database/fileset"

# Label types mapping
LABEL_TYPES = {
    '1': 'arc',                              # Regular arc
    '2': 'weak_arc',                        # Weak arc  
    '3': 'restriking_arc',                  # Restriking arc
    '4': 'parallel_motor_arc',              # Parallel motor arc
    '5': 'negative_transient',              # Non-arc transient
    '6': 'steady_state',                    # No arc activity
    '7': 'restriking_arc_parallel_motor',   # Restriking arc with parallel motor
    '8': 'parallel_motor_continuous',       # Parallel motor continuous arc
    '0': 'other'                            # Other/unknown
}

# Arc type augmentation schemes
ARC_AUGMENTATION_SCHEMES = {
    'arc': ['no_arc_steady_state', 'arc_transient', 'continuous_arc'],
    'weak_arc': ['no_arc_steady_state', 'weak_arc_transient', 'continuous_arc'],
    'restriking_arc': ['no_arc_steady_state', 'arc_transient', 'arc_restrike', 'arc_transient', 'continuous_arc'],
    'parallel_motor_arc': ['no_arc_steady_state', 'arc_transient', 'continuous_arc'],  # Motor context
    'negative_transient': ['no_arc_steady_state', 'negative_transient', 'no_arc_steady_state'],
    'steady_state': ['steady_state'],
    'restriking_arc_parallel_motor': ['motor_steady_state', 'arc_transient', 'arc_restrike', 'arc_transient', 'continuous_arc'],  # Restriking arc with motor
    'parallel_motor_continuous': ['motor_steady_state', 'continuous_arc'],  # Continuous arc with motor
    'other': ['unknown']
}

class DatabaseConnectionPool:
    """Thread-safe SQLite connection pool for better performance"""
    
    def __init__(self, db_path, pool_size=10, timeout=30):
        self.db_path = db_path
        self.pool_size = pool_size
        self.timeout = timeout
        self.pool = Queue(maxsize=pool_size)
        self.lock = threading.Lock()
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize the connection pool"""
        for _ in range(self.pool_size):
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            self.pool.put(conn)
    
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool"""
        conn = None
        try:
            conn = self.pool.get(timeout=self.timeout)
            yield conn
        except Empty:
            raise Exception("Database connection pool exhausted")
        finally:
            if conn:
                self.pool.put(conn)
    
    def close_all(self):
        """Close all connections in the pool"""
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.close()
            except Empty:
                break

# Global connection pool instance
_connection_pool = None

def get_connection_pool():
    """Get or create the global connection pool"""
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = DatabaseConnectionPool(DATABASE_PATH)
    return _connection_pool

class V3Database:
    """Database interface for V3 arc detection system"""
    
    def __init__(self, db_path=DATABASE_PATH):
        self.db_path = db_path
        self.binary_dir = BINARY_DATA_DIR
        self.pool = get_connection_pool()
    
    def get_connection(self):
        """Get database connection from pool"""
        return self.pool.get_connection()
    
    def get_all_files(self, label_filter=None):
        """Get all files, optionally filtered by label"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            if label_filter:
                cursor.execute('''
                    SELECT file_id, original_filename, original_path, selected_label,
                           transient1_index, transient2_index, transient3_index,
                           voltage_level, current_level, binary_data_path
                    FROM files WHERE selected_label = ? ORDER BY file_id
                ''', (label_filter,))
            else:
                cursor.execute('''
                    SELECT file_id, original_filename, original_path, selected_label,
                           transient1_index, transient2_index, transient3_index,
                           voltage_level, current_level, binary_data_path
                    FROM files ORDER BY file_id
                ''')
            
            return cursor.fetchall()
    
    def get_file_by_id(self, file_id):
        """Get file information by file_id"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT file_id, original_filename, original_path, selected_label,
                       transient1_index, transient2_index, transient3_index,
                       voltage_level, current_level, binary_data_path,
                       total_samples, sampling_rate
                FROM files WHERE file_id = ?
            ''', (file_id,))
            
            return cursor.fetchone()
    
    def load_file_data(self, file_id):
        """Load load voltage and source current data for a file"""
        file_info = self.get_file_by_id(file_id)
        if not file_info:
            return None, None
        
        binary_filename = file_info[9]  # binary_data_path
        binary_path = os.path.join(self.binary_dir, binary_filename)
        
        if not os.path.exists(binary_path):
            print(f"Binary file not found: {binary_path}")
            return None, None
        
        try:
            # Load NumPy array [samples, 2] - [load_voltage, source_current]
            data = np.load(binary_path)
            load_voltage_data = data[:, 0]
            source_current_data = data[:, 1]
            return load_voltage_data, source_current_data
        except Exception as e:
            print(f"Error loading binary data: {e}")
            return None, None
    
    def update_file_label(self, file_id, new_label):
        """Update file label"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE files SET selected_label = ?, updated_at = CURRENT_TIMESTAMP
                WHERE file_id = ?
            ''', (new_label, file_id))
            
            conn.commit()
            print(f"Updated file {file_id} label to: {new_label}")
    
    def update_transient_indices(self, file_id, transient1=None, transient2=None, transient3=None):
        """Update transient indices for a file"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE files SET 
                transient1_index = ?, 
                transient2_index = ?, 
                transient3_index = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE file_id = ?
        ''', (transient1, transient2, transient3, file_id))
        
        conn.commit()
        conn.close()
        print(f"Updated file {file_id} transient indices: {transient1}, {transient2}, {transient3}")
    
    def update_experiment_status(self, file_id, status, manual_reviewed=True, reviewer_notes=None, reviewer_name=None, confidence=None):
        """Update experiment status in the new status table"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Check if status record exists
            cursor.execute('SELECT file_id FROM experiment_status WHERE file_id = ?', (file_id,))
            exists = cursor.fetchone()
            
            if exists:
                # Update existing record
                cursor.execute('''
                    UPDATE experiment_status SET 
                        status = ?, 
                        manual_reviewed = ?, 
                        reviewer_notes = ?,
                        reviewed_by = ?,
                        classification_confidence = ?,
                        reviewed_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE file_id = ?
                ''', (status, 1 if manual_reviewed else 0, reviewer_notes, reviewer_name, confidence, file_id))
            else:
                # Insert new record
                cursor.execute('''
                    INSERT INTO experiment_status 
                    (file_id, status, manual_reviewed, reviewer_notes, reviewed_by, classification_confidence, reviewed_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (file_id, status, 1 if manual_reviewed else 0, reviewer_notes, reviewer_name, confidence))
            
            conn.commit()
            print(f"Updated experiment {file_id} status to: {status}")
            return True
            
        except Exception as e:
            print(f"Error updating experiment status: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def get_experiment_status(self, file_id):
        """Get experiment status information"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT status, manual_reviewed, reviewer_notes, reviewed_by, 
                   classification_confidence, reviewed_at, created_at, updated_at
            FROM experiment_status WHERE file_id = ?
        ''', (file_id,))
        
        result = cursor.fetchone()
        conn.close()
        return result
    
    def get_files_by_status(self, status=None, manual_reviewed=None):
        """Get files filtered by status and/or manual review flag"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        query = '''
            SELECT f.file_id, f.original_filename, f.original_path, f.selected_label,
                   f.transient1_index, f.transient2_index, f.transient3_index,
                   f.voltage_level, f.current_level, f.binary_data_path,
                   es.status, es.manual_reviewed, es.reviewer_notes, es.reviewed_at
            FROM files f
            LEFT JOIN experiment_status es ON f.file_id = es.file_id
            WHERE 1=1
        '''
        params = []
        
        if status is not None:
            query += ' AND es.status = ?'
            params.append(status)
        
        if manual_reviewed is not None:
            query += ' AND es.manual_reviewed = ?'
            params.append(1 if manual_reviewed else 0)
        
        query += ' ORDER BY f.file_id'
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        return results
    
    def get_status_summary(self):
        """Get summary statistics for experiment status"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Status counts
        cursor.execute('''
            SELECT status, COUNT(*) as count 
            FROM experiment_status 
            GROUP BY status 
            ORDER BY count DESC
        ''')
        status_counts = cursor.fetchall()
        
        # Manual review counts
        cursor.execute('''
            SELECT manual_reviewed, COUNT(*) as count 
            FROM experiment_status 
            GROUP BY manual_reviewed
        ''')
        review_counts = cursor.fetchall()
        
        # Recent activity
        cursor.execute('''
            SELECT COUNT(*) as recent_reviews
            FROM experiment_status 
            WHERE reviewed_at >= datetime('now', '-24 hours')
        ''')
        recent_reviews = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'status_counts': status_counts,
            'review_counts': review_counts,
            'recent_reviews': recent_reviews
        }

    def add_rejection(self, file_id):
        """Legacy method - now uses status table"""
        return self.update_experiment_status(file_id, 'reject', manual_reviewed=True, 
                                           reviewer_notes='Rejected via legacy method')
    
    def get_rejected_files(self):
        """Get all rejected files"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT r.rejection_id, r.file_id, r.filename, r.original_path, 
                   r.original_label, r.rejected_at
            FROM rejections r
            ORDER BY r.rejected_at DESC
        ''')
        
        rejections = cursor.fetchall()
        conn.close()
        return rejections
    
    def get_augmentation_scheme(self, label):
        """Get augmentation scheme for a label type"""
        return ARC_AUGMENTATION_SCHEMES.get(label, ['unknown'])
    
    def get_label_statistics(self):
        """Get statistics about labels in database"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT selected_label, COUNT(*) as count
            FROM files 
            GROUP BY selected_label 
            ORDER BY count DESC
        ''')
        
        stats = cursor.fetchall()
        conn.close()
        return stats
    
    def search_files(self, filename_pattern=None, voltage_range=None, current_range=None):
        """Search files by various criteria"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        query = "SELECT file_id, original_filename, selected_label, voltage_level, current_level FROM files WHERE 1=1"
        params = []
        
        if filename_pattern:
            query += " AND original_filename LIKE ?"
            params.append(f"%{filename_pattern}%")
        
        if voltage_range:
            query += " AND voltage_level BETWEEN ? AND ?"
            params.extend(voltage_range)
        
        if current_range:
            query += " AND current_level BETWEEN ? AND ?"
            params.extend(current_range)
        
        query += " ORDER BY file_id"
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        return results

# Convenience functions
def get_database():
    """Get database instance"""
    return V3Database()

def load_file_data(file_id):
    """Load data for a file ID
    
    Returns:
        tuple: (load_voltage_data, source_current_data) - both as numpy arrays
    """
    db = get_database()
    return db.load_file_data(file_id)

def update_label(file_id, label_key):
    """Update file label using key mapping"""
    if label_key in LABEL_TYPES:
        db = get_database()
        db.update_file_label(file_id, LABEL_TYPES[label_key])
        return True
    return False

def propagate_label_change(file_id, new_label):
    """Propagate label change to other tools"""
    # This function can be used to notify other tools of label changes
    # Could use file-based communication, HTTP requests, or message queues
    print(f"Propagating label change: file_id={file_id}, new_label={new_label}")
    
    # Future: Send updates to transient viewer and augmented generator
    # Example: HTTP POST to running tools with update notification

if __name__ == "__main__":
    # Test database connection
    db = get_database()
    try:
        stats = db.get_label_statistics()
        print("Database connection successful!")
        print("Label statistics:")
        for label, count in stats:
            print(f"  {label}: {count}")
    except Exception as e:
        print(f"Database connection failed: {e}")