#!/usr/bin/env python3
"""
Task 3: Python Basics
Build a Python script to read multiple CSV files, clean missing values, and merge datasets.
"""

import csv
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CSVDataProcessor:
    """Process multiple CSV files for resume analysis."""
    
    def __init__(self, data_dir: str = "data/csv"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.processed_data = []
        
    def create_sample_csv_files(self):
        """Create sample CSV files for demonstration."""
        # Sample resumes data
        resumes_data = [
            ['id', 'name', 'email', 'phone', 'experience_years', 'skills'],
            ['1', 'John Doe', 'john@email.com', '555-0101', '5', 'Python,SQL,Spark'],
            ['2', 'Jane Smith', 'jane@email.com', '555-0102', '3', 'Java,Python,AWS'],
            ['3', 'Bob Johnson', '', '555-0103', '', 'C++,JavaScript'],
            ['4', 'Alice Brown', 'alice@email.com', '', '7', 'Python,Machine Learning'],
            ['5', 'Charlie Wilson', 'charlie@email.com', '555-0105', '2', ''],
        ]
        
        # Sample job descriptions
        jobs_data = [
            ['job_id', 'title', 'company', 'required_skills', 'min_experience'],
            ['J001', 'Data Engineer', 'TechCorp', 'Python,SQL,Spark', '3'],
            ['J002', 'ML Engineer', 'AI Solutions', 'Python,Machine Learning', '4'],
            ['J003', 'Software Dev', 'SoftInc', 'Java,Python', '2'],
            ['J004', 'Cloud Architect', 'CloudSys', 'AWS,Azure', '5'],
            ['J005', 'Data Analyst', 'DataCo', 'SQL,Python,Tableau', '2'],
        ]
        
        # Sample skills mapping
        skills_data = [
            ['skill', 'category', 'proficiency_level'],
            ['Python', 'Programming', 'Advanced'],
            ['SQL', 'Database', 'Intermediate'],
            ['Spark', 'Big Data', 'Advanced'],
            ['Java', 'Programming', 'Intermediate'],
            ['AWS', 'Cloud', 'Intermediate'],
            ['Machine Learning', 'AI/ML', 'Advanced'],
            ['C++', 'Programming', 'Beginner'],
            ['JavaScript', 'Web', 'Intermediate'],
            ['Azure', 'Cloud', 'Beginner'],
            ['Tableau', 'Visualization', 'Intermediate'],
        ]
        
        # Write CSV files
        files = {
            'resumes.csv': resumes_data,
            'jobs.csv': jobs_data,
            'skills.csv': skills_data
        }
        
        for filename, data in files.items():
            filepath = self.data_dir / filename
            with open(filepath, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(data)
            logger.info(f"Created sample file: {filepath}")
    
    def read_csv_file(self, filename: str) -> List[Dict[str, Any]]:
        """Read a CSV file and return list of dictionaries."""
        filepath = self.data_dir / filename
        data = []
        
        try:
            with open(filepath, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    data.append(dict(row))
            logger.info(f"Read {len(data)} rows from {filename}")
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
        except Exception as e:
            logger.error(f"Error reading {filename}: {e}")
        
        return data
    
    def read_multiple_csv(self, filenames: List[str]) -> Dict[str, List[Dict]]:
        """Read multiple CSV files."""
        all_data = {}
        for filename in filenames:
            all_data[filename.replace('.csv', '')] = self.read_csv_file(filename)
        return all_data
    
    def clean_missing_values(self, data: List[Dict], 
                            fill_strategy: str = 'default',
                            default_values: Dict = None) -> List[Dict]:
        """Clean missing values in dataset."""
        if default_values is None:
            default_values = {
                'email': 'not_provided@example.com',
                'phone': '000-000-0000',
                'experience_years': '0',
                'skills': 'Not specified'
            }
        
        cleaned_data = []
        missing_count = 0
        
        for row in data:
            cleaned_row = {}
            for key, value in row.items():
                # Check for missing/empty values
                if value is None or value == '' or value.strip() == '':
                    missing_count += 1
                    if fill_strategy == 'default':
                        cleaned_row[key] = default_values.get(key, 'Unknown')
                    elif fill_strategy == 'drop':
                        continue
                    elif fill_strategy == 'interpolate':
                        # Simple interpolation for numeric fields
                        if key in ['experience_years']:
                            cleaned_row[key] = self._interpolate_value(data, key)
                        else:
                            cleaned_row[key] = default_values.get(key, 'Unknown')
                else:
                    cleaned_row[key] = value
            cleaned_data.append(cleaned_row)
        
        logger.info(f"Cleaned {missing_count} missing values using {fill_strategy} strategy")
        return cleaned_data
    
    def _interpolate_value(self, data: List[Dict], field: str) -> str:
        """Simple interpolation for missing numeric values."""
        values = []
        for row in data:
            try:
                if row.get(field) and row[field].strip():
                    values.append(float(row[field]))
            except (ValueError, TypeError):
                pass
        
        if values:
            return str(sum(values) / len(values))
        return '0'
    
    def merge_datasets(self, left_data: List[Dict], right_data: List[Dict],
                      left_key: str, right_key: str,
                      merge_type: str = 'inner') -> List[Dict]:
        """Merge two datasets based on a common key."""
        merged_data = []
        
        # Create lookup for right dataset
        right_lookup = {}
        for row in right_data:
            key_value = row.get(right_key)
            if key_value:
                if key_value not in right_lookup:
                    right_lookup[key_value] = []
                right_lookup[key_value].append(row)
        
        # Perform merge
        for left_row in left_data:
            left_key_value = left_row.get(left_key)
            matching_rows = right_lookup.get(left_key_value, [])
            
            if matching_rows:
                for right_row in matching_rows:
                    merged_row = {**left_row, **right_row}
                    merged_data.append(merged_row)
            elif merge_type in ['left', 'outer']:
                merged_data.append(left_row)
        
        # Add unmatched right rows for outer join
        if merge_type in ['right', 'outer']:
            matched_keys = set(row.get(left_key) for row in left_data if row.get(left_key))
            for right_row in right_data:
                if right_row.get(right_key) not in matched_keys:
                    merged_data.append(right_row)
        
        logger.info(f"Merged datasets: {len(left_data)} + {len(right_data)} = {len(merged_data)} rows")
        return merged_data
    
    def process_resume_data(self) -> Dict:
        """Complete processing pipeline for resume data."""
        # Create sample files
        self.create_sample_csv_files()
        
        # Read all CSV files
        csv_files = ['resumes.csv', 'jobs.csv', 'skills.csv']
        all_data = self.read_multiple_csv(csv_files)
        
        # Clean missing values
        cleaned_resumes = self.clean_missing_values(
            all_data['resumes'], 
            fill_strategy='default'
        )
        
        # Transform skills from string to list
        for resume in cleaned_resumes:
            skills_str = resume.get('skills', '')
            resume['skills_list'] = [s.strip() for s in skills_str.split(',') if s.strip()]
        
        # Merge with skills data (example: enriching resume with skill categories)
        enriched_data = self.merge_datasets(
            cleaned_resumes,
            all_data['skills'],
            left_key='skills',
            right_key='skill',
            merge_type='left'
        )
        
        # Calculate statistics
        stats = {
            'total_resumes': len(cleaned_resumes),
            'avg_experience': sum(float(r.get('experience_years', 0) or 0) 
                                 for r in cleaned_resumes) / len(cleaned_resumes),
            'unique_skills': len(set(skill for r in cleaned_resumes 
                                    for skill in r.get('skills_list', []))),
            'missing_values_filled': sum(1 for r in cleaned_resumes 
                                        for v in r.values() if 'not_provided' in str(v) 
                                        or 'Unknown' in str(v) or '000-000' in str(v))
        }
        
        return {
            'raw_data': all_data,
            'cleaned_resumes': cleaned_resumes,
            'enriched_data': enriched_data,
            'statistics': stats
        }


def main():
    """Main execution for Task 3."""
    processor = CSVDataProcessor()
    result = processor.process_resume_data()
    
    logger.info("Task 3 completed successfully!")
    
    print("\n=== Processing Statistics ===")
    print(f"Total Resumes: {result['statistics']['total_resumes']}")
    print(f"Average Experience: {result['statistics']['avg_experience']:.1f} years")
    print(f"Unique Skills: {result['statistics']['unique_skills']}")
    print(f"Missing Values Filled: {result['statistics']['missing_values_filled']}")
    
    return result


if __name__ == "__main__":
    main()
