#!/usr/bin/env python3
"""
Task 11: Data Ingestion
Develop batch ingestion pipeline from CSV to database.
"""

import pandas as pd
import sqlite3
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
import hashlib
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BatchIngestionPipeline:
    """Batch data ingestion pipeline with validation and monitoring."""
    
    def __init__(self, db_path: str = "data/ingestion_warehouse.db"):
        self.db_path = db_path
        self.conn = None
        self.ingestion_log = []
        self.stats = {
            'files_processed': 0,
            'records_read': 0,
            'records_inserted': 0,
            'records_rejected': 0,
            'errors': []
        }
        
    def connect(self):
        """Connect to the database."""
        import os
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        logger.info(f"Connected to database: {self.db_path}")
    
    def disconnect(self):
        """Disconnect from the database."""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from database")
    
    def create_target_tables(self):
        """Create target tables for ingested data."""
        cursor = self.conn.cursor()
        
        # Main candidates table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS candidates (
                candidate_id TEXT PRIMARY KEY,
                full_name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                location TEXT,
                experience_years INTEGER,
                current_salary INTEGER,
                expected_salary INTEGER,
                skills TEXT,
                education TEXT,
                source_file TEXT,
                ingestion_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                record_hash TEXT,
                is_valid BOOLEAN DEFAULT 1
            )
        ''')
        
        # Rejected records table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rejected_records (
                rejection_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_file TEXT,
                record_data TEXT,
                rejection_reason TEXT,
                rejection_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Ingestion audit log
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ingestion_audit (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT,
                file_hash TEXT,
                start_time DATETIME,
                end_time DATETIME,
                records_read INTEGER,
                records_inserted INTEGER,
                records_rejected INTEGER,
                status TEXT
            )
        ''')
        
        self.conn.commit()
        logger.info("Target tables created")
    
    def generate_sample_csv_files(self, output_dir: str = "data/csv_input"):
        """Generate sample CSV files for ingestion."""
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Chris', 'Jessica']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']
        locations = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Seattle', 'Boston']
        skills_pool = ['Python', 'SQL', 'Spark', 'AWS', 'Azure', 'Docker', 'Kubernetes', 'ML']
        education_levels = ['High School', 'Bachelor', 'Master', 'PhD']
        
        # Generate multiple CSV files
        for file_num in range(5):
            records = []
            for i in range(1000):
                # Introduce some data quality issues randomly
                has_issues = random.random() < 0.05  # 5% bad records
                
                record = {
                    'candidate_id': f'CAND{file_num}{str(i).zfill(5)}',
                    'full_name': f'{random.choice(first_names)} {random.choice(last_names)}',
                    'email': f'candidate{file_num}{i}@email.com' if not has_issues else 'invalid-email',
                    'phone': f'555-{random.randint(1000, 9999)}',
                    'location': random.choice(locations),
                    'experience_years': random.randint(0, 20) if not has_issues else -5,
                    'current_salary': random.randint(40000, 150000),
                    'expected_salary': random.randint(50000, 200000),
                    'skills': ','.join(random.sample(skills_pool, random.randint(2, 5))),
                    'education': random.choice(education_levels)
                }
                records.append(record)
            
            df = pd.DataFrame(records)
            file_path = f'{output_dir}/candidates_batch_{file_num+1}.csv'
            df.to_csv(file_path, index=False)
            logger.info(f"Generated {file_path} with {len(records)} records")
    
    def calculate_file_hash(self, file_path: str) -> str:
        """Calculate MD5 hash of file for duplicate detection."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def validate_record(self, record: Dict) -> tuple[bool, str]:
        """Validate a single record."""
        errors = []
        
        # Required fields
        if not record.get('candidate_id'):
            errors.append("Missing candidate_id")
        
        if not record.get('full_name'):
            errors.append("Missing full_name")
        
        # Email validation
        email = record.get('email', '')
        if '@' not in email or '.' not in email:
            errors.append("Invalid email format")
        
        # Numeric validation
        experience = record.get('experience_years')
        if experience is not None:
            try:
                exp_val = int(experience)
                if exp_val < 0 or exp_val > 50:
                    errors.append("Experience years out of range")
            except (ValueError, TypeError):
                errors.append("Invalid experience_years")
        
        # Salary validation
        current = record.get('current_salary')
        expected = record.get('expected_salary')
        if current and expected:
            try:
                if int(expected) < int(current) * 0.8:
                    errors.append("Expected salary too low")
            except (ValueError, TypeError):
                pass
        
        if errors:
            return False, '; '.join(errors)
        return True, ""
    
    def ingest_file(self, file_path: str, batch_size: int = 100) -> Dict:
        """Ingest a single CSV file."""
        file_name = Path(file_path).name
        file_hash = self.calculate_file_hash(file_path)
        
        logger.info(f"Starting ingestion of {file_name}")
        start_time = datetime.now()
        
        audit_record = {
            'file_name': file_name,
            'file_hash': file_hash,
            'start_time': start_time,
            'records_read': 0,
            'records_inserted': 0,
            'records_rejected': 0,
            'status': 'in_progress'
        }
        
        # Read CSV in chunks
        records_read = 0
        records_inserted = 0
        records_rejected = 0
        
        cursor = self.conn.cursor()
        
        for chunk in pd.read_csv(file_path, chunksize=batch_size):
            records_read += len(chunk)
            
            for _, row in chunk.iterrows():
                record = row.to_dict()
                
                # Validate record
                is_valid, error_msg = self.validate_record(record)
                
                if is_valid:
                    # Calculate record hash for deduplication
                    record_str = json.dumps(record, sort_keys=True)
                    record_hash = hashlib.md5(record_str.encode()).hexdigest()
                    
                    try:
                        cursor.execute('''
                            INSERT OR REPLACE INTO candidates 
                            (candidate_id, full_name, email, phone, location, experience_years,
                             current_salary, expected_salary, skills, education, source_file, record_hash)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            record.get('candidate_id'),
                            record.get('full_name'),
                            record.get('email'),
                            record.get('phone'),
                            record.get('location'),
                            record.get('experience_years'),
                            record.get('current_salary'),
                            record.get('expected_salary'),
                            record.get('skills'),
                            record.get('education'),
                            file_name,
                            record_hash
                        ))
                        records_inserted += 1
                    except Exception as e:
                        records_rejected += 1
                        cursor.execute('''
                            INSERT INTO rejected_records (source_file, record_data, rejection_reason)
                            VALUES (?, ?, ?)
                        ''', (file_name, json.dumps(record), str(e)))
                else:
                    records_rejected += 1
                    cursor.execute('''
                        INSERT INTO rejected_records (source_file, record_data, rejection_reason)
                        VALUES (?, ?, ?)
                    ''', (file_name, json.dumps(record), error_msg))
            
            self.conn.commit()
        
        end_time = datetime.now()
        
        # Update audit log
        cursor.execute('''
            INSERT INTO ingestion_audit 
            (file_name, file_hash, start_time, end_time, records_read, records_inserted, records_rejected, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (file_name, file_hash, start_time, end_time, records_read, 
              records_inserted, records_rejected, 'completed'))
        
        self.conn.commit()
        
        result = {
            'file_name': file_name,
            'records_read': records_read,
            'records_inserted': records_inserted,
            'records_rejected': records_rejected,
            'duration_seconds': (end_time - start_time).total_seconds()
        }
        
        logger.info(f"Completed ingestion of {file_name}: {records_inserted} inserted, {records_rejected} rejected")
        
        return result
    
    def ingest_directory(self, input_dir: str = "data/csv_input") -> Dict:
        """Ingest all CSV files from a directory."""
        import os
        
        input_path = Path(input_dir)
        csv_files = sorted(input_path.glob("*.csv"))
        
        if not csv_files:
            logger.warning(f"No CSV files found in {input_dir}")
            return {'status': 'no_files'}
        
        logger.info(f"Found {len(csv_files)} CSV files to ingest")
        
        results = []
        for csv_file in csv_files:
            result = self.ingest_file(str(csv_file))
            results.append(result)
            
            # Update stats
            self.stats['files_processed'] += 1
            self.stats['records_read'] += result['records_read']
            self.stats['records_inserted'] += result['records_inserted']
            self.stats['records_rejected'] += result['records_rejected']
        
        return {
            'files_processed': len(results),
            'total_records_read': self.stats['records_read'],
            'total_records_inserted': self.stats['records_inserted'],
            'total_records_rejected': self.stats['records_rejected'],
            'file_results': results
        }
    
    def get_ingestion_summary(self) -> Dict:
        """Get summary of ingestion operations."""
        cursor = self.conn.cursor()
        
        # Get counts from main table
        cursor.execute('SELECT COUNT(*) FROM candidates')
        total_candidates = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM rejected_records')
        total_rejected = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT source_file) FROM candidates')
        source_files = cursor.fetchone()[0]
        
        # Get audit log
        cursor.execute('''
            SELECT file_name, records_read, records_inserted, records_rejected, status
            FROM ingestion_audit ORDER BY audit_id DESC
        ''')
        audit_log = [dict(row) for row in cursor.fetchall()]
        
        return {
            'total_candidates': total_candidates,
            'total_rejected': total_rejected,
            'source_files': source_files,
            'audit_log': audit_log
        }
    
    def run_demo(self) -> Dict:
        """Run the complete batch ingestion demonstration."""
        self.connect()
        self.create_target_tables()
        self.generate_sample_csv_files()
        
        # Run ingestion
        ingestion_result = self.ingest_directory()
        
        # Get summary
        summary = self.get_ingestion_summary()
        
        self.disconnect()
        
        logger.info("Task 11 completed successfully!")
        
        return {
            'ingestion_result': ingestion_result,
            'summary': summary
        }


def main():
    """Main execution for Task 11."""
    pipeline = BatchIngestionPipeline()
    results = pipeline.run_demo()
    
    print("\n=== Batch Ingestion Results ===")
    print(f"Files Processed: {results['ingestion_result']['files_processed']}")
    print(f"Total Records Read: {results['ingestion_result']['total_records_read']:,}")
    print(f"Total Records Inserted: {results['ingestion_result']['total_records_inserted']:,}")
    print(f"Total Records Rejected: {results['ingestion_result']['total_records_rejected']:,}")
    
    print("\n=== Database Summary ===")
    print(f"Total Candidates in Database: {results['summary']['total_candidates']:,}")
    print(f"Total Rejected Records: {results['summary']['total_rejected']:,}")
    
    return results


if __name__ == "__main__":
    main()
