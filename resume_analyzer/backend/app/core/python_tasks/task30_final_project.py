#!/usr/bin/env python3
"""
Task 30: Final Project
Design end-to-end pipeline: ingestion → processing → storage → orchestration → dashboard.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
import random
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EndToEndPipeline:
    """Complete end-to-end data pipeline for Resume Analyzer."""
    
    def __init__(self):
        self.pipeline_name = "ResumeAnalyzer_ETL_Pipeline"
        self.components = {}
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'records_processed': 0,
            'errors': []
        }
    
    def generate_resume_data(self, n: int = 1000) -> List[Dict]:
        """Generate sample resume data."""
        first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Chris', 'Jessica']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']
        skills_pool = ['Python', 'Java', 'SQL', 'Spark', 'AWS', 'Azure', 'Docker', 'Kubernetes', 
                      'Machine Learning', 'Deep Learning', 'TensorFlow', 'PyTorch', 'Pandas',
                      'NumPy', 'Scikit-learn', 'Tableau', 'PowerBI', 'Excel', 'R', 'Scala']
        locations = ['New York', 'San Francisco', 'Seattle', 'Austin', 'Boston', 'Chicago']
        job_titles = ['Data Engineer', 'Data Scientist', 'ML Engineer', 'Software Engineer',
                     'Data Analyst', 'BI Developer', 'Cloud Architect', 'DevOps Engineer']
        
        resumes = []
        for i in range(n):
            experience = random.randint(0, 20)
            resumes.append({
                'candidate_id': f'CAND{str(i+1).zfill(5)}',
                'name': f'{random.choice(first_names)} {random.choice(last_names)}',
                'email': f'candidate{i+1}@email.com',
                'location': random.choice(locations),
                'job_title': random.choice(job_titles),
                'experience_years': experience,
                'current_salary': random.randint(50000, 200000),
                'expected_salary': random.randint(60000, 250000),
                'skills': random.sample(skills_pool, random.randint(3, 8)),
                'education': random.choice(['High School', 'Bachelor', 'Master', 'PhD']),
                'is_active': random.choice([True, True, True, False]),
                'last_updated': (datetime.now() - timedelta(days=random.randint(1, 365))).isoformat()
            })
        
        return resumes
    
    def stage_ingestion(self, data: List[Dict]) -> Dict:
        """Stage 1: Data Ingestion"""
        logger.info("="*60)
        logger.info("STAGE 1: DATA INGESTION")
        logger.info("="*60)
        
        # Simulate batch ingestion
        start_time = time.time()
        
        # Validate incoming data
        valid_records = []
        rejected_records = []
        
        for record in data:
            if self._validate_record(record):
                valid_records.append(record)
            else:
                rejected_records.append(record)
        
        # Store in raw zone (simulated S3)
        raw_storage = {
            'path': 's3://resume-analyzer-data/raw/',
            'records': len(valid_records),
            'format': 'json'
        }
        
        duration = time.time() - start_time
        
        result = {
            'stage': 'ingestion',
            'records_received': len(data),
            'records_valid': len(valid_records),
            'records_rejected': len(rejected_records),
            'storage_location': raw_storage['path'],
            'duration_seconds': duration
        }
        
        logger.info(f"Ingested {result['records_valid']} records in {duration:.2f}s")
        
        return result
    
    def _validate_record(self, record: Dict) -> bool:
        """Validate a single record."""
        required_fields = ['candidate_id', 'name', 'email']
        return all(record.get(f) for f in required_fields)
    
    def stage_processing(self, data: List[Dict]) -> Dict:
        """Stage 2: Data Processing with Spark"""
        logger.info("="*60)
        logger.info("STAGE 2: DATA PROCESSING")
        logger.info("="*60)
        
        start_time = time.time()
        
        # Transformations
        processed = []
        
        for record in data:
            # Clean and enrich
            processed_record = {
                'candidate_id': record['candidate_id'],
                'name': record['name'].strip().title(),
                'email': record['email'].lower().strip(),
                'location': record['location'],
                'job_title': record['job_title'],
                'experience_years': record['experience_years'],
                'experience_level': self._categorize_experience(record['experience_years']),
                'current_salary': record['current_salary'],
                'expected_salary': record['expected_salary'],
                'salary_expectation_pct': round(
                    (record['expected_salary'] - record['current_salary']) / record['current_salary'] * 100, 2
                ),
                'skills': record['skills'],
                'skill_count': len(record['skills']),
                'primary_skill': record['skills'][0] if record['skills'] else None,
                'education': record['education'],
                'is_active': record['is_active'],
                'last_updated': record['last_updated'],
                'processing_date': datetime.now().isoformat()
            }
            processed.append(processed_record)
        
        # Aggregations
        location_stats = {}
        for record in processed:
            loc = record['location']
            if loc not in location_stats:
                location_stats[loc] = {'count': 0, 'avg_salary': 0}
            location_stats[loc]['count'] += 1
            location_stats[loc]['avg_salary'] += record['expected_salary']
        
        for loc in location_stats:
            location_stats[loc]['avg_salary'] = round(
                location_stats[loc]['avg_salary'] / location_stats[loc]['count']
            )
        
        duration = time.time() - start_time
        
        result = {
            'stage': 'processing',
            'records_processed': len(processed),
            'transformations': [
                'Data cleaning and standardization',
                'Experience level categorization',
                'Salary expectation calculation',
                'Skill count aggregation',
                'Location-based statistics'
            ],
            'aggregations': {
                'location_stats': location_stats
            },
            'duration_seconds': duration
        }
        
        logger.info(f"Processed {result['records_processed']} records in {duration:.2f}s")
        
        return result
    
    def _categorize_experience(self, years: int) -> str:
        """Categorize experience level."""
        if years < 3:
            return 'Junior'
        elif years < 7:
            return 'Mid-level'
        else:
            return 'Senior'
    
    def stage_storage(self, data: List[Dict]) -> Dict:
        """Stage 3: Data Storage"""
        logger.info("="*60)
        logger.info("STAGE 3: DATA STORAGE")
        logger.info("="*60)
        
        start_time = time.time()
        
        # Store in different layers
        storage_info = {
            'processed_zone': {
                'path': 's3://resume-analyzer-data/processed/',
                'format': 'parquet',
                'records': len(data)
            },
            'data_warehouse': {
                'platform': 'BigQuery',
                'dataset': 'resume_analytics',
                'table': 'candidates_processed',
                'records': len(data)
            },
            'data_lake': {
                'platform': 'Delta Lake',
                'path': '/lakehouse/candidates/',
                'version': 1
            }
        }
        
        duration = time.time() - start_time
        
        result = {
            'stage': 'storage',
            'storage_layers': storage_info,
            'duration_seconds': duration
        }
        
        logger.info(f"Stored data in {len(storage_info)} layers in {duration:.2f}s")
        
        return result
    
    def stage_orchestration(self) -> Dict:
        """Stage 4: Pipeline Orchestration with Airflow"""
        logger.info("="*60)
        logger.info("STAGE 4: ORCHESTRATION")
        logger.info("="*60)
        
        dag_structure = {
            'dag_id': 'resume_analyzer_pipeline',
            'schedule_interval': '0 2 * * *',  # Daily at 2 AM
            'tasks': [
                {
                    'task_id': 'extract_from_sources',
                    'type': 'PythonOperator',
                    'dependencies': [],
                    'retries': 3
                },
                {
                    'task_id': 'validate_raw_data',
                    'type': 'PythonOperator',
                    'dependencies': ['extract_from_sources'],
                    'retries': 1
                },
                {
                    'task_id': 'transform_with_spark',
                    'type': 'SparkSubmitOperator',
                    'dependencies': ['validate_raw_data'],
                    'retries': 2
                },
                {
                    'task_id': 'load_to_warehouse',
                    'type': 'PythonOperator',
                    'dependencies': ['transform_with_spark'],
                    'retries': 3
                },
                {
                    'task_id': 'update_dashboard',
                    'type': 'PythonOperator',
                    'dependencies': ['load_to_warehouse'],
                    'retries': 1
                },
                {
                    'task_id': 'send_success_notification',
                    'type': 'EmailOperator',
                    'dependencies': ['update_dashboard'],
                    'retries': 1
                }
            ]
        }
        
        result = {
            'stage': 'orchestration',
            'dag_structure': dag_structure,
            'monitoring': {
                'alerts': ['Email on failure', 'Slack notification'],
                'metrics': ['Task duration', 'Success rate', 'Data quality scores']
            }
        }
        
        logger.info(f"Orchestration DAG: {dag_structure['dag_id']}")
        
        return result
    
    def stage_dashboard(self) -> Dict:
        """Stage 5: Analytics Dashboard"""
        logger.info("="*60)
        logger.info("STAGE 5: DASHBOARD")
        logger.info("="*60)
        
        dashboard_config = {
            'platform': 'Apache Superset / Tableau',
            'title': 'Resume Analyzer Analytics',
            'kpis': [
                {
                    'name': 'Total Candidates',
                    'value': '10,000+',
                    'trend': '+15%'
                },
                {
                    'name': 'Average Experience',
                    'value': '5.2 years',
                    'trend': '+3%'
                },
                {
                    'name': 'Top Skill',
                    'value': 'Python',
                    'trend': 'Stable'
                }
            ],
            'charts': [
                {
                    'title': 'Candidates by Location',
                    'type': 'bar_chart',
                    'data_source': 'location_stats'
                },
                {
                    'title': 'Experience Distribution',
                    'type': 'histogram',
                    'data_source': 'experience_years'
                },
                {
                    'title': 'Top Skills',
                    'type': 'word_cloud',
                    'data_source': 'skills'
                },
                {
                    'title': 'Salary Trends',
                    'type': 'line_chart',
                    'data_source': 'expected_salary'
                }
            ],
            'filters': [
                'Location',
                'Experience Level',
                'Job Title',
                'Date Range'
            ]
        }
        
        result = {
            'stage': 'dashboard',
            'config': dashboard_config,
            'refresh_schedule': 'Every 1 hour'
        }
        
        logger.info(f"Dashboard: {dashboard_config['title']}")
        
        return result
    
    def run_complete_pipeline(self) -> Dict:
        """Run the complete end-to-end pipeline."""
        logger.info("\n" + "="*60)
        logger.info(f"STARTING PIPELINE: {self.pipeline_name}")
        logger.info("="*60)
        
        self.metrics['start_time'] = datetime.now()
        
        # Generate source data
        source_data = self.generate_resume_data(1000)
        
        # Stage 1: Ingestion
        ingestion_result = self.stage_ingestion(source_data)
        
        # Stage 2: Processing
        processing_result = self.stage_processing(source_data)
        
        # Stage 3: Storage
        storage_result = self.stage_storage(source_data)
        
        # Stage 4: Orchestration
        orchestration_result = self.stage_orchestration()
        
        # Stage 5: Dashboard
        dashboard_result = self.stage_dashboard()
        
        self.metrics['end_time'] = datetime.now()
        self.metrics['records_processed'] = len(source_data)
        
        # Calculate total duration
        total_duration = (
            self.metrics['end_time'] - self.metrics['start_time']
        ).total_seconds()
        
        logger.info("\n" + "="*60)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*60)
        
        return {
            'pipeline_name': self.pipeline_name,
            'execution_summary': {
                'start_time': self.metrics['start_time'].isoformat(),
                'end_time': self.metrics['end_time'].isoformat(),
                'total_duration_seconds': total_duration,
                'records_processed': self.metrics['records_processed']
            },
            'stages': {
                'ingestion': ingestion_result,
                'processing': processing_result,
                'storage': storage_result,
                'orchestration': orchestration_result,
                'dashboard': dashboard_result
            },
            'architecture': {
                'data_sources': ['Resume Database', 'Job Portals', 'LinkedIn API'],
                'ingestion_layer': 'Apache Kafka / AWS Kinesis',
                'processing_layer': 'Apache Spark on EMR',
                'storage_layer': 'S3 + Delta Lake + BigQuery',
                'orchestration': 'Apache Airflow',
                'serving_layer': 'Superset Dashboard + REST API'
            }
        }
    
    def get_architecture_diagram(self) -> str:
        """Get ASCII architecture diagram."""
        return """
┌─────────────────────────────────────────────────────────────────┐
│                      RESUME ANALYZER PIPELINE                    │
└─────────────────────────────────────────────────────────────────┘

  ┌──────────────┐
  │ Data Sources │  Resume DB, Job Portals, APIs
  └──────┬───────┘
         │
         ▼
  ┌──────────────┐     ┌──────────────┐
  │   Kafka /    │────▶│  Raw Zone    │ (S3)
  │   Kinesis    │     │   (JSON)     │
  └──────┬───────┘     └──────────────┘
         │
         ▼
  ┌──────────────┐     ┌──────────────┐
  │Apache Spark  │────▶│Processed Zone│ (Parquet)
  │   (EMR)      │     │   (Delta)    │
  └──────┬───────┘     └──────────────┘
         │
         ▼
  ┌──────────────┐     ┌──────────────┐
  │  BigQuery /  │────▶│  Data Mart   │ (Warehouse)
  │   Redshift   │     │   (Views)    │
  └──────┬───────┘     └──────────────┘
         │
         ▼
  ┌──────────────┐     ┌──────────────┐
  │  Superset /  │◀────│   Airflow    │ (Orchestration)
  │   Tableau    │     │   (DAGs)     │
  └──────────────┘     └──────────────┘
        """


def main():
    """Main execution for Task 30."""
    pipeline = EndToEndPipeline()
    results = pipeline.run_complete_pipeline()
    
    print("\n" + "="*70)
    print("END-TO-END PIPELINE EXECUTION RESULTS")
    print("="*70)
    
    print(f"\nPipeline Name: {results['pipeline_name']}")
    print(f"Total Duration: {results['execution_summary']['total_duration_seconds']:.2f}s")
    print(f"Records Processed: {results['execution_summary']['records_processed']:,}")
    
    print("\nStage Results:")
    for stage_name, stage_result in results['stages'].items():
        if 'duration_seconds' in stage_result:
            print(f"  {stage_name.upper()}: {stage_result['duration_seconds']:.2f}s")
    
    print("\n" + pipeline.get_architecture_diagram())
    
    return results


if __name__ == "__main__":
    main()
