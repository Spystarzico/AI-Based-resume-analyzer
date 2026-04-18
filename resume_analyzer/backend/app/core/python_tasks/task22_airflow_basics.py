#!/usr/bin/env python3
"""
Task 22: Airflow Basics
Create DAG for ETL pipeline.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Callable
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AirflowTask:
    """Simulate an Airflow task."""
    
    def __init__(self, task_id: str, python_callable: Callable, 
                 dependencies: List[str] = None, retries: int = 1):
        self.task_id = task_id
        self.python_callable = python_callable
        self.dependencies = dependencies or []
        self.retries = retries
        self.status = 'pending'
        self.execution_date = None
        self.duration = None
        self.logs = []
        
    def execute(self, context: Dict = None) -> Any:
        """Execute the task."""
        self.status = 'running'
        self.execution_date = datetime.now()
        
        logger.info(f"Executing task: {self.task_id}")
        
        try:
            start = datetime.now()
            result = self.python_callable(context or {})
            self.duration = (datetime.now() - start).total_seconds()
            self.status = 'success'
            self.logs.append(f"Task succeeded in {self.duration:.2f}s")
            return result
        except Exception as e:
            self.status = 'failed'
            self.logs.append(f"Task failed: {str(e)}")
            raise


class AirflowDAG:
    """Simulate an Airflow DAG."""
    
    def __init__(self, dag_id: str, schedule_interval: str, 
                 start_date: datetime, catchup: bool = False):
        self.dag_id = dag_id
        self.schedule_interval = schedule_interval
        self.start_date = start_date
        self.catchup = catchup
        self.tasks = {}
        self.default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        }
        
    def add_task(self, task: AirflowTask):
        """Add a task to the DAG."""
        self.tasks[task.task_id] = task
        logger.info(f"Added task {task.task_id} to DAG {self.dag_id}")
    
    def get_task_dependencies(self) -> Dict:
        """Get task dependency graph."""
        return {
            task_id: task.dependencies 
            for task_id, task in self.tasks.items()
        }
    
    def topological_sort(self) -> List[str]:
        """Sort tasks in topological order."""
        # Simple topological sort
        in_degree = {t: 0 for t in self.tasks}
        
        for task in self.tasks.values():
            for dep in task.dependencies:
                if dep in in_degree:
                    in_degree[task.task_id] += 1
        
        queue = [t for t, d in in_degree.items() if d == 0]
        result = []
        
        while queue:
            task_id = queue.pop(0)
            result.append(task_id)
            
            for t, task in self.tasks.items():
                if task_id in task.dependencies:
                    in_degree[t] -= 1
                    if in_degree[t] == 0:
                        queue.append(t)
        
        return result
    
    def run(self, execution_date: datetime = None) -> Dict:
        """Run the DAG."""
        execution_date = execution_date or datetime.now()
        
        logger.info(f"Running DAG: {self.dag_id}")
        logger.info(f"Execution date: {execution_date}")
        
        # Get execution order
        execution_order = self.topological_sort()
        logger.info(f"Execution order: {execution_order}")
        
        results = {}
        context = {'execution_date': execution_date, 'ds': execution_date.strftime('%Y-%m-%d')}
        
        for task_id in execution_order:
            task = self.tasks[task_id]
            
            # Check dependencies
            deps_satisfied = all(
                self.tasks[dep].status == 'success' 
                for dep in task.dependencies
            )
            
            if not deps_satisfied:
                logger.warning(f"Skipping {task_id} - dependencies not satisfied")
                continue
            
            # Execute task
            try:
                result = task.execute(context)
                results[task_id] = {
                    'status': 'success',
                    'duration': task.duration,
                    'result': result
                }
            except Exception as e:
                results[task_id] = {
                    'status': 'failed',
                    'error': str(e)
                }
        
        return results


class ETLPipelineDAG:
    """Create ETL pipeline DAG."""
    
    def __init__(self):
        self.data = {}
        
    def extract_task(self, context: Dict) -> Dict:
        """Extract data from source."""
        logger.info("Extracting data...")
        
        # Simulate data extraction
        self.data['raw'] = [
            {'id': i, 'name': f'User{i}', 'value': random.randint(1, 100)}
            for i in range(100)
        ]
        
        return {
            'records_extracted': len(self.data['raw']),
            'source': 'database',
            'timestamp': datetime.now().isoformat()
        }
    
    def transform_task(self, context: Dict) -> Dict:
        """Transform extracted data."""
        logger.info("Transforming data...")
        
        raw_data = self.data.get('raw', [])
        
        # Apply transformations
        transformed = []
        for record in raw_data:
            transformed.append({
                'id': record['id'],
                'name': record['name'].upper(),
                'value': record['value'],
                'category': 'high' if record['value'] > 50 else 'low',
                'processed_at': datetime.now().isoformat()
            })
        
        self.data['transformed'] = transformed
        
        return {
            'records_transformed': len(transformed),
            'transformations': ['uppercase', 'categorize', 'add_timestamp']
        }
    
    def validate_task(self, context: Dict) -> Dict:
        """Validate transformed data."""
        logger.info("Validating data...")
        
        transformed = self.data.get('transformed', [])
        
        errors = []
        for record in transformed:
            if not record.get('name'):
                errors.append(f"Missing name for id {record['id']}")
            if record.get('value', 0) < 0:
                errors.append(f"Negative value for id {record['id']}")
        
        self.data['valid'] = len(errors) == 0
        
        return {
            'records_validated': len(transformed),
            'errors': errors,
            'is_valid': len(errors) == 0
        }
    
    def load_task(self, context: Dict) -> Dict:
        """Load data to target."""
        logger.info("Loading data...")
        
        if not self.data.get('valid'):
            raise ValueError("Data validation failed, cannot load")
        
        transformed = self.data.get('transformed', [])
        
        # Simulate loading to warehouse
        self.data['loaded'] = transformed
        
        return {
            'records_loaded': len(transformed),
            'target': 'data_warehouse',
            'timestamp': datetime.now().isoformat()
        }
    
    def notify_task(self, context: Dict) -> Dict:
        """Send notification."""
        logger.info("Sending notification...")
        
        return {
            'notification_sent': True,
            'recipients': ['data-team@company.com'],
            'message': f"ETL completed for {context.get('ds', 'today')}"
        }
    
    def create_dag(self) -> AirflowDAG:
        """Create the ETL DAG."""
        dag = AirflowDAG(
            dag_id='etl_resume_pipeline',
            schedule_interval='0 2 * * *',  # Daily at 2 AM
            start_date=datetime(2024, 1, 1),
            catchup=False
        )
        
        # Create tasks
        extract = AirflowTask(
            task_id='extract_data',
            python_callable=self.extract_task,
            retries=2
        )
        
        transform = AirflowTask(
            task_id='transform_data',
            python_callable=self.transform_task,
            dependencies=['extract_data']
        )
        
        validate = AirflowTask(
            task_id='validate_data',
            python_callable=self.validate_task,
            dependencies=['transform_data']
        )
        
        load = AirflowTask(
            task_id='load_data',
            python_callable=self.load_task,
            dependencies=['validate_data']
        )
        
        notify = AirflowTask(
            task_id='send_notification',
            python_callable=self.notify_task,
            dependencies=['load_data']
        )
        
        # Add tasks to DAG
        dag.add_task(extract)
        dag.add_task(transform)
        dag.add_task(validate)
        dag.add_task(load)
        dag.add_task(notify)
        
        return dag


class AirflowBasicsDemo:
    """Demonstrate Airflow basics."""
    
    def __init__(self):
        pass
    
    def get_airflow_concepts(self) -> Dict:
        """Get Airflow core concepts."""
        return {
            'core_concepts': {
                'dag': 'Directed Acyclic Graph - collection of tasks',
                'task': 'Unit of work within a DAG',
                'operator': 'Template for tasks (PythonOperator, BashOperator, etc.)',
                'hook': 'Connection to external systems',
                'sensor': 'Wait for external events',
                'xcom': 'Cross-communication between tasks'
            },
            'scheduling': {
                'schedule_interval': 'When DAG runs (cron expression)',
                'execution_date': 'Logical date of DAG run',
                'start_date': 'When DAG begins scheduling',
                'catchup': 'Whether to run missed intervals'
            },
            'task_lifecycle': {
                'none': 'Task not yet queued',
                'scheduled': 'Task waiting in scheduler',
                'queued': 'Task waiting in executor',
                'running': 'Task executing',
                'success': 'Task completed successfully',
                'failed': 'Task failed',
                'upstream_failed': 'Upstream task failed',
                'skipped': 'Task skipped'
            }
        }
    
    def run_demo(self) -> Dict:
        """Run complete Airflow basics demonstration."""
        # Create ETL DAG
        etl_pipeline = ETLPipelineDAG()
        dag = etl_pipeline.create_dag()
        
        # Run the DAG
        results = dag.run()
        
        concepts = self.get_airflow_concepts()
        
        logger.info("Task 22 completed successfully!")
        
        return {
            'dag_info': {
                'dag_id': dag.dag_id,
                'schedule_interval': dag.schedule_interval,
                'tasks': list(dag.tasks.keys()),
                'dependencies': dag.get_task_dependencies()
            },
            'execution_results': results,
            'concepts': concepts
        }


def main():
    """Main execution for Task 22."""
    demo = AirflowBasicsDemo()
    results = demo.run_demo()
    
    print("\n=== Airflow DAG: ETL Pipeline ===")
    
    print(f"\nDAG ID: {results['dag_info']['dag_id']}")
    print(f"Schedule: {results['dag_info']['schedule_interval']}")
    
    print("\nTasks:")
    for task_id in results['dag_info']['tasks']:
        deps = results['dag_info']['dependencies'].get(task_id, [])
        dep_str = f" (depends on: {', '.join(deps)})" if deps else ""
        print(f"  - {task_id}{dep_str}")
    
    print("\nExecution Results:")
    for task_id, result in results['execution_results'].items():
        status = result.get('status', 'unknown')
        duration = result.get('duration', 'N/A')
        print(f"  {task_id}: {status} ({duration}s)" if duration != 'N/A' else f"  {task_id}: {status}")
    
    return results


if __name__ == "__main__":
    main()
