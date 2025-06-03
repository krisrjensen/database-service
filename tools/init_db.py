#!/usr/bin/env python3
"""
V3 Database Creation and Migration Script
Creates centralized database with binary storage for arc detection data
"""

import os
import sqlite3
import numpy as np
import scipy.io
import hashlib
import time
from pathlib import Path
import glob
import json

# Configuration
DATABASE_PATH = "/Volumes/ArcData/V3_database/arc_detection.db"
BINARY_DATA_DIR = "/Volumes/ArcData/V3_database/fileset"
SOURCE_DATA_DIR = os.getenv("SOURCE_DATA_DIR", "/Volumes/ArcData/V3_raw_data")

# Label mapping
LABEL_DIRECTORIES = {
    'arc_matrix_experiment': 'arc',
    'arc_matrix_experiment_with_parallel_motor': 'parallel_motor_arc', 
    'transient_negative_test': 'negative_transient',
    'steady_state': 'steady_state'
}

def create_database_schema():
    """Create the database schema"""
    print("Creating database schema...")
    
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    # Main files table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            file_id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_filename TEXT NOT NULL,
            original_path TEXT NOT NULL,
            original_label_directory TEXT,
            selected_label TEXT DEFAULT 'unknown',
            transient1_index INTEGER,
            transient2_index INTEGER,
            transient3_index INTEGER,
            voltage_level REAL,
            current_level REAL,
            datestamp TEXT,
            binary_data_path TEXT NOT NULL,
            data_checksum TEXT,
            sampling_rate REAL DEFAULT 5000000,
            total_samples INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(original_path, original_filename)
        )
    ''')
    
    # Rejections table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rejections (
            rejection_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER REFERENCES files(file_id),
            filename TEXT,
            original_path TEXT,
            original_label TEXT,
            rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create indexes for performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_label ON files(selected_label)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_path ON files(original_path)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_rejections_file_id ON rejections(file_id)')
    
    conn.commit()
    conn.close()
    print(f"Database created: {DATABASE_PATH}")

def extract_experiment_info(filepath):
    """Extract experiment information from file path and name"""
    path_parts = Path(filepath).parts
    filename = Path(filepath).name
    
    # Extract label directory from path
    label_dir = None
    for part in path_parts:
        if part in LABEL_DIRECTORIES:
            label_dir = part
            break
    
    # Extract voltage/current from filename if available
    voltage = None
    current = None
    datestamp = None
    
    # Pattern: YYYYMMDD_HHMMSS_350V_1976mA_experiment_x_x
    if '_' in filename:
        parts = filename.replace('.mat', '').split('_')
        for part in parts:
            if 'V' in part and part.replace('V', '').isdigit():
                voltage = float(part.replace('V', ''))
            elif 'mA' in part and part.replace('mA', '').replace('.', '').isdigit():
                current = float(part.replace('mA', ''))
            elif len(part) == 8 and part.isdigit():  # YYYYMMDD
                datestamp = part
    
    return label_dir, voltage, current, datestamp

def load_channel_data(filepath):
    """Load data from a single channel .mat file"""
    try:
        mat_data = scipy.io.loadmat(filepath)
        
        # Look for 'data' key first (most common)
        if 'data' in mat_data:
            data = mat_data['data'].flatten()
        else:
            # Look for numerical data arrays (skip string metadata)
            data_arrays = []
            for key, value in mat_data.items():
                if not key.startswith('_') and hasattr(value, 'dtype'):
                    # Check if it's numerical data with reasonable size
                    if value.dtype.kind in 'biufc' and value.size > 1000:
                        data_arrays.append((key, value))
            
            if not data_arrays:
                print(f"No numerical data found in {filepath}")
                return None
            
            # Use the largest numerical array
            key, data_array = max(data_arrays, key=lambda x: x[1].size)
            data = data_array.flatten()
            print(f"  Using key '{key}' with {len(data)} samples")
        
        # Limit to 2.5M samples (0.5s at 5MSPS)
        max_samples = int(2.5e6)
        if len(data) > max_samples:
            data = data[:max_samples]
        
        return data
        
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None

def save_binary_data(load_voltage_data, source_current_data, file_id):
    """Save load voltage and source current data as binary .npy file"""
    if load_voltage_data is None or source_current_data is None:
        return None
    
    # Combine into 2D array [samples, 2] - [load_voltage, source_current]
    combined_data = np.column_stack([load_voltage_data, source_current_data])
    
    # Generate binary file path with 8-digit format
    binary_filename = f"{file_id:08d}.npy"
    binary_path = os.path.join(BINARY_DATA_DIR, binary_filename)
    
    # Save as NumPy array
    np.save(binary_path, combined_data)
    
    # Calculate checksum
    checksum = hashlib.md5(combined_data.tobytes()).hexdigest()
    
    return binary_filename, checksum

def migrate_mat_files():
    """Migrate .mat files to database and binary storage"""
    print(f"Scanning experiment directories in: {SOURCE_DATA_DIR}")
    
    # Find experiment directories (each contains ch1.mat and ch4.mat)
    experiment_dirs = []
    for root, dirs, files in os.walk(SOURCE_DATA_DIR):
        # Check if directory contains both ch1.mat (load voltage) and ch4.mat (source current)
        ch1_file = None
        ch4_file = None
        for file in files:
            if file.endswith('_ch1.mat'):
                ch1_file = os.path.join(root, file)
            elif file.endswith('_ch4.mat'):
                ch4_file = os.path.join(root, file)
        
        if ch1_file and ch4_file:
            experiment_dirs.append((root, ch1_file, ch4_file))
    
    print(f"Found {len(experiment_dirs)} experiment directories")
    
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    processed_count = 0
    error_count = 0
    
    for experiment_dir, ch1_file, ch4_file in experiment_dirs:
        try:
            experiment_name = os.path.basename(experiment_dir)
            print(f"Processing: {experiment_name}")
            
            # Extract experiment info from directory path
            label_dir, voltage, current, datestamp = extract_experiment_info(experiment_dir)
            
            # Map label directory to selected label
            selected_label = LABEL_DIRECTORIES.get(label_dir, 'unknown')
            
            # Load load voltage data from ch1 and source current data from ch4
            print(f"  Loading load voltage (ch1): {ch1_file}")
            load_voltage_data = load_channel_data(ch1_file)
            print(f"  Loading source current (ch4): {ch4_file}")
            source_current_data = load_channel_data(ch4_file)
            
            if load_voltage_data is None or source_current_data is None:
                print(f"  Failed to load data from {experiment_name}")
                error_count += 1
                continue
            
            # Ensure same length
            min_len = min(len(load_voltage_data), len(source_current_data))
            load_voltage_data = load_voltage_data[:min_len]
            source_current_data = source_current_data[:min_len]
            
            # Insert file record (will get file_id)
            cursor.execute('''
                INSERT INTO files (
                    original_filename, original_path, original_label_directory,
                    selected_label, voltage_level, current_level, datestamp,
                    binary_data_path, total_samples
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                experiment_name,
                experiment_dir,
                label_dir,
                selected_label,
                voltage,
                current,
                datestamp,
                'pending',  # Will update after saving binary
                min_len
            ))
            
            file_id = cursor.lastrowid
            
            # Save binary data
            binary_filename, checksum = save_binary_data(load_voltage_data, source_current_data, file_id)
            
            if binary_filename:
                # Update record with binary path and checksum
                cursor.execute('''
                    UPDATE files SET binary_data_path = ?, data_checksum = ?
                    WHERE file_id = ?
                ''', (binary_filename, checksum, file_id))
                
                print(f"  → file_id: {file_id}, binary: {binary_filename}")
                processed_count += 1
            else:
                print(f"  Failed to save binary data for {experiment_name}")
                error_count += 1
            
            # Commit every 10 files
            if processed_count % 10 == 0:
                conn.commit()
                print(f"  Progress: {processed_count} processed, {error_count} errors")
        
        except Exception as e:
            print(f"Error processing {experiment_name}: {e}")
            error_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"\nMigration complete:")
    print(f"  Processed: {processed_count} files")
    print(f"  Errors: {error_count} files")
    print(f"  Database: {DATABASE_PATH}")
    print(f"  Binary files: {BINARY_DATA_DIR}")

def create_summary_report():
    """Generate summary report of migrated data"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    print("\n=== DATABASE SUMMARY ===")
    
    # Total files
    cursor.execute("SELECT COUNT(*) FROM files")
    total_files = cursor.fetchone()[0]
    print(f"Total files: {total_files}")
    
    # Files by label
    cursor.execute("SELECT selected_label, COUNT(*) FROM files GROUP BY selected_label ORDER BY COUNT(*) DESC")
    label_counts = cursor.fetchall()
    print("\nFiles by label:")
    for label, count in label_counts:
        print(f"  {label}: {count}")
    
    # Sample statistics
    cursor.execute("SELECT AVG(total_samples), MIN(total_samples), MAX(total_samples) FROM files")
    avg_samples, min_samples, max_samples = cursor.fetchone()
    print(f"\nSample statistics:")
    print(f"  Average samples: {avg_samples:.0f}")
    print(f"  Min samples: {min_samples}")
    print(f"  Max samples: {max_samples}")
    
    # Voltage/current ranges
    cursor.execute("SELECT AVG(voltage_level), MIN(voltage_level), MAX(voltage_level) FROM files WHERE voltage_level IS NOT NULL")
    result = cursor.fetchone()
    if result[0]:
        print(f"\nVoltage range: {result[1]:.0f}V - {result[2]:.0f}V (avg: {result[0]:.0f}V)")
    
    cursor.execute("SELECT AVG(current_level), MIN(current_level), MAX(current_level) FROM files WHERE current_level IS NOT NULL")
    result = cursor.fetchone()
    if result[0]:
        print(f"Current range: {result[1]:.0f}mA - {result[2]:.0f}mA (avg: {result[0]:.0f}mA)")
    
    conn.close()

def main():
    """Main migration process"""
    print("=== V3 Database Migration ===")
    
    # Ensure directories exist
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    os.makedirs(BINARY_DATA_DIR, exist_ok=True)
    
    # Create database schema
    create_database_schema()
    
    # Migrate .mat files
    migrate_mat_files()
    
    # Generate summary
    create_summary_report()
    
    print(f"\nMigration complete!")

if __name__ == "__main__":
    main()