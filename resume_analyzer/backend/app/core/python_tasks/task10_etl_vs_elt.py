#!/usr/bin/env python3
"""
Task 10: ETL vs ELT
Build both ETL and ELT pipelines and compare performance.
"""

import pandas as pd
import numpy as np
import sqlite3
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Any
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """Traditional ETL: Extract -> Transform -> Load"""
    
    def __init__(self):
        self.execution_time = 0
        self.records_processed = 0
        self.errors = []
        
    def extract(self, source_path: str) -> pd.DataFrame:
        """Extract data from source."""
        logger.info(f"Extracting data from {source_path}")
        
        # Simulate reading from multiple sources
        df = pd.read_csv(source_path)
        logger.info(f"Extracted {len(df)} records")
        return df
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform data before loading."""
        logger.info("Transforming data...")
        
        # Data cleaning
        df = df.drop_duplicates()
        
        # Handle missing values
        df['email'] = df['email'].fillna('unknown@example.com')
        df['phone'] = df['phone'].fillna('000-000-0000')
        df['experience_years'] = df['experience_years'].fillna(0)
        
        # Data type conversions
        df['experience_years'] = pd.to_numeric(df['experience_years'], errors='coerce')
        df['salary'] = pd.to_numeric(df['salary'], errors='coerce')
        
        # Standardization
        df['email'] = df['email'].str.lower().str.strip()
        df['name'] = df['name'].str.title().str.strip()
        
        # Derived columns
        df['experience_level'] = df['experience_years'].apply(
            lambda x: 'Senior' if x >= 7 else 'Mid' if x >= 3 else 'Junior'
        )
        
        # Data validation
        df = df[df['email'].str.contains('@', na=False)]
        df = df[df['experience_years'] >= 0]
        
        # Business rules
        df['salary_category'] = pd.cut(df['salary'], 
                                       bins=[0, 50000, 100000, 150000, float('inf')],
                                       labels=['Low', 'Medium', 'High', 'Premium'])
        
        logger.info(f"Transformed to {len(df)} records")
        return df
    
    def load(self, df: pd.DataFrame, target_path: str):
        """Load transformed data to target."""
        logger.info(f"Loading data to {target_path}")
        
        # Load to data warehouse
        conn = sqlite3.connect(target_path)
        df.to_sql('processed_candidates', conn, if_exists='replace', index=False)
        conn.close()
        
        logger.info(f"Loaded {len(df)} records")
        self.records_processed = len(df)
    
    def run(self, source_path: str, target_path: str) -> Dict:
        """Run the complete ETL pipeline."""
        start_time = time.time()
        
        # Extract
        raw_data = self.extract(source_path)
        
        # Transform
        transformed_data = self.transform(raw_data)
        
        # Load
        self.load(transformed_data, target_path)
        
        self.execution_time = time.time() - start_time
        
        return {
            'pipeline_type': 'ETL',
            'execution_time': self.execution_time,
            'records_processed': self.records_processed,
            'errors': self.errors
        }


class ELTPipeline:
    """Modern ELT: Extract -> Load -> Transform"""
    
    def __init__(self):
        self.execution_time = 0
        self.records_processed = 0
        self.errors = []
        
    def extract(self, source_path: str) -> pd.DataFrame:
        """Extract data from source."""
        logger.info(f"Extracting data from {source_path}")
        
        df = pd.read_csv(source_path)
        logger.info(f"Extracted {len(df)} records")
        return df
    
    def load(self, df: pd.DataFrame, target_path: str):
        """Load raw data to target (data lake/warehouse)."""
        logger.info(f"Loading raw data to {target_path}")
        
        conn = sqlite3.connect(target_path)
        
        # Load raw data as-is
        df.to_sql('raw_candidates', conn, if_exists='replace', index=False)
        
        logger.info(f"Loaded {len(df)} raw records")
        conn.close()
    
    def transform(self, target_path: str):
        """Transform data within the target system using SQL."""
        logger.info("Transforming data in target system...")
        
        conn = sqlite3.connect(target_path)
        cursor = conn.cursor()
        
        # Create transformed view/table using SQL
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_candidates AS
            SELECT 
                id,
                UPPER(TRIM(name)) as name,
                LOWER(TRIM(email)) as email,
                COALESCE(NULLIF(TRIM(phone), ''), '000-000-0000') as phone,
                COALESCE(CAST(experience_years AS REAL), 0) as experience_years,
                COALESCE(CAST(salary AS REAL), 0) as salary,
                CASE 
                    WHEN COALESCE(CAST(experience_years AS REAL), 0) >= 7 THEN 'Senior'
                    WHEN COALESCE(CAST(experience_years AS REAL), 0) >= 3 THEN 'Mid'
                    ELSE 'Junior'
                END as experience_level,
                CASE 
                    WHEN COALESCE(CAST(salary AS REAL), 0) < 50000 THEN 'Low'
                    WHEN COALESCE(CAST(salary AS REAL), 0) < 100000 THEN 'Medium'
                    WHEN COALESCE(CAST(salary AS REAL), 0) < 150000 THEN 'High'
                    ELSE 'Premium'
                END as salary_category,
                CASE 
                    WHEN LOWER(email) LIKE '%@%' THEN 1
                    ELSE 0
                END as is_valid_email
            FROM raw_candidates
            WHERE COALESCE(CAST(experience_years AS REAL), 0) >= 0
        ''')
        
        # Get count
        cursor.execute('SELECT COUNT(*) FROM processed_candidates')
        count = cursor.fetchone()[0]
        
        conn.commit()
        conn.close()
        
        logger.info(f"Transformed {count} records")
        self.records_processed = count
    
    def run(self, source_path: str, target_path: str) -> Dict:
        """Run the complete ELT pipeline."""
        start_time = time.time()
        
        # Extract
        raw_data = self.extract(source_path)
        
        # Load (raw)
        self.load(raw_data, target_path)
        
        # Transform (in target)
        self.transform(target_path)
        
        self.execution_time = time.time() - start_time
        
        return {
            'pipeline_type': 'ELT',
            'execution_time': self.execution_time,
            'records_processed': self.records_processed,
            'errors': self.errors
        }


class ETLvsELTDemo:
    """Demonstrate and compare ETL vs ELT approaches."""
    
    def __init__(self):
        self.data_dir = 'data'
        self.source_file = f'{self.data_dir}/raw_candidates.csv'
        self.etl_target = f'{self.data_dir}/etl_warehouse.db'
        self.elt_target = f'{self.data_dir}/elt_warehouse.db'
        
    def generate_sample_data(self, n_records: int = 10000):
        """Generate sample raw data."""
        import os
        os.makedirs(self.data_dir, exist_ok=True)
        
        first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Chris', 'Jessica']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']
        
        data = []
        for i in range(n_records):
            # Introduce some data quality issues
            name = f'{random.choice(first_names)} {random.choice(last_names)}'
            email = f'user{i}@example.com' if random.random() > 0.1 else ''  # 10% missing emails
            phone = f'555-{random.randint(1000, 9999)}' if random.random() > 0.15 else ''  # 15% missing phones
            experience = random.randint(0, 20) if random.random() > 0.05 else ''  # 5% missing experience
            salary = random.randint(30000, 200000)
            
            data.append({
                'id': i + 1,
                'name': name,
                'email': email,
                'phone': phone,
                'experience_years': experience,
                'salary': salary
            })
        
        df = pd.DataFrame(data)
        df.to_csv(self.source_file, index=False)
        logger.info(f"Generated {n_records} sample records")
        
    def compare_approaches(self) -> Dict:
        """Compare ETL and ELT approaches."""
        
        # Run ETL pipeline
        logger.info("\n" + "="*50)
        logger.info("Running ETL Pipeline")
        logger.info("="*50)
        etl = ETLPipeline()
        etl_result = etl.run(self.source_file, self.etl_target)
        
        # Run ELT pipeline
        logger.info("\n" + "="*50)
        logger.info("Running ELT Pipeline")
        logger.info("="*50)
        elt = ELTPipeline()
        elt_result = elt.run(self.source_file, self.elt_target)
        
        return {
            'etl_result': etl_result,
            'elt_result': elt_result,
            'comparison': {
                'speed': {
                    'etl': etl_result['execution_time'],
                    'elt': elt_result['execution_time'],
                    'winner': 'ELT' if elt_result['execution_time'] < etl_result['execution_time'] else 'ETL'
                },
                'flexibility': {
                    'etl': 'Low - transformations are hardcoded',
                    'elt': 'High - SQL-based transformations can be modified easily'
                },
                'scalability': {
                    'etl': 'Limited by processing server',
                    'elt': 'High - leverages database engine power'
                },
                'data_quality': {
                    'etl': 'High - validated before loading',
                    'elt': 'Medium - raw data preserved, quality checks in SQL'
                }
            }
        }
    
    def get_architecture_comparison(self) -> Dict:
        """Get detailed architecture comparison."""
        return {
            'etl_architecture': {
                'workflow': [
                    '1. Extract from source systems',
                    '2. Transform in staging area (external server)',
                    '3. Load clean data to data warehouse'
                ],
                'pros': [
                    'Clean, structured data in warehouse',
                    'Better data quality control',
                    'Optimized storage',
                    'Security - sensitive data masked before loading'
                ],
                'cons': [
                    'Slower for large datasets',
                    'Less flexible - need to redefine pipeline for new transformations',
                    'Expensive processing infrastructure',
                    'Data loss - raw data not preserved'
                ],
                'best_for': [
                    'Small to medium datasets',
                    'Strict data governance requirements',
                    'Legacy systems',
                    'Complex transformations not supported by target database'
                ]
            },
            'elt_architecture': {
                'workflow': [
                    '1. Extract from source systems',
                    '2. Load raw data to data lake/warehouse',
                    '3. Transform using SQL within the warehouse'
                ],
                'pros': [
                    'Fast initial load',
                    'Highly flexible - transformations can be changed anytime',
                    'Preserves raw data for reprocessing',
                    'Scales with cloud data warehouse',
                    'Self-service analytics friendly'
                ],
                'cons': [
                    'Requires powerful target database',
                    'Storage costs for raw data',
                    'Potential security concerns with raw data',
                    'May require data governance discipline'
                ],
                'best_for': [
                    'Large datasets',
                    'Cloud data warehouses (Snowflake, BigQuery, Redshift)',
                    'Agile environments',
                    'Data science use cases needing raw data'
                ]
            }
        }
    
    def run_demo(self) -> Dict:
        """Run the complete ETL vs ELT demonstration."""
        self.generate_sample_data(n_records=10000)
        comparison = self.compare_approaches()
        architecture = self.get_architecture_comparison()
        
        logger.info("Task 10 completed successfully!")
        
        return {
            'performance_comparison': comparison,
            'architecture_comparison': architecture
        }


def main():
    """Main execution for Task 10."""
    demo = ETLvsELTDemo()
    results = demo.run_demo()
    
    print("\n=== ETL vs ELT Performance Comparison ===")
    etl_time = results['performance_comparison']['etl_result']['execution_time']
    elt_time = results['performance_comparison']['elt_result']['execution_time']
    print(f"ETL Execution Time: {etl_time:.4f} seconds")
    print(f"ELT Execution Time: {elt_time:.4f} seconds")
    print(f"Winner: {results['performance_comparison']['comparison']['speed']['winner']}")
    
    print("\n=== Architecture Comparison ===")
    print("\nETL Best For:")
    for item in results['architecture_comparison']['etl_architecture']['best_for']:
        print(f"  - {item}")
    
    print("\nELT Best For:")
    for item in results['architecture_comparison']['elt_architecture']['best_for']:
        print(f"  - {item}")
    
    return results


if __name__ == "__main__":
    main()
