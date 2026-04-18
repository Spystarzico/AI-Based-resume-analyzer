#!/usr/bin/env python3
"""
Task 23: Airflow Advanced
Add scheduling, monitoring, retry mechanisms.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Callable
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AdvancedAirflowTask:
    """Advanced Airflow task with monitoring and retries."""
    
    def __init__(self, task_id: str, python_callable: Callable,
                 dependencies: List[str] = None, retries: int = 3,
                 retry_delay: int = 300, timeout: int = 3600,
                 email_on_failure: bool = False, sla: timedelta = None):
        self.task_id = task_id
        self.python_callable = python_callable
        self.dependencies = dependencies or []
        self.retries = retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.email_on_failure = email_on_failure
        self.sla = sla
        
        self.status = 'pending'
        self.execution_date = None
        self.duration = None
        self.retry_count = 0
        self.logs = []
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'cpu_usage': 0,
            'memory_usage': 0
        }
        
    def execute(self, context: Dict = None) -> Any:
        """Execute task with retry logic."""
        context = context or {}
        
        for attempt in range(self.retries + 1):
            self.retry_count = attempt
            self.status = 'running'
            self.metrics['start_time'] = datetime.now()
            
            logger.info(f"Executing task {self.task_id} (attempt {attempt + 1}/{self.retries + 1})")
            
            try:
                # Simulate task execution
                start = datetime.now()
                result = self.python_callable(context)
                self.duration = (datetime.now() - start).total_seconds()
                
                # Check SLA
                if self.sla and self.duration > self.sla.total_seconds():
                    self.logs.append(f"WARNING: Task exceeded SLA of {self.sla}")
                
                self.status = 'success'
                self.metrics['end_time'] = datetime.now()
                self.logs.append(f"Task succeeded in {self.duration:.2f}s")
                
                return result
                
            except Exception as e:
                self.logs.append(f"Attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < self.retries:
                    logger.warning(f"Task {self.task_id} failed, retrying in {self.retry_delay}s...")
                    time.sleep(0.1)  # Simulate retry delay
                else:
                    self.status = 'failed'
                    self.metrics['end_time'] = datetime.now()
                    
                    if self.email_on_failure:
                        self.logs.append(f"Sent failure notification to task owner")
                    
                    raise


class AirflowScheduler:
    """Simulate Airflow scheduler."""
    
    def __init__(self):
        self.dags = {}
        self.dag_runs = {}
        self.schedule = {}
        
    def register_dag(self, dag_id: str, dag):
        """Register a DAG with the scheduler."""
        self.dags[dag_id] = dag
        logger.info(f"Registered DAG: {dag_id}")
    
    def schedule_dag(self, dag_id: str, execution_date: datetime):
        """Schedule a DAG run."""
        if dag_id not in self.dag_runs:
            self.dag_runs[dag_id] = []
        
        dag_run = {
            'execution_date': execution_date,
            'state': 'scheduled',
            'run_id': f"scheduled__{execution_date.isoformat()}"
        }
        
        self.dag_runs[dag_id].append(dag_run)
        logger.info(f"Scheduled DAG run: {dag_id} for {execution_date}")
        
        return dag_run
    
    def get_next_execution(self, schedule_interval: str, last_execution: datetime) -> datetime:
        """Calculate next execution date based on schedule."""
        # Simplified - just add 1 day for daily
        if schedule_interval == '0 2 * * *':  # Daily at 2 AM
            return last_execution + timedelta(days=1)
        elif schedule_interval == '0 */6 * * *':  # Every 6 hours
            return last_execution + timedelta(hours=6)
        elif schedule_interval == '0 0 * * 0':  # Weekly
            return last_execution + timedelta(weeks=1)
        else:
            return last_execution + timedelta(days=1)


class AirflowMonitor:
    """Monitor Airflow tasks and DAGs."""
    
    def __init__(self):
        self.metrics = {
            'task_runs': [],
            'dag_runs': [],
            'failures': []
        }
        
    def record_task_run(self, task_id: str, dag_id: str, 
                       execution_date: datetime, duration: float,
                       status: str):
        """Record task execution metrics."""
        self.metrics['task_runs'].append({
            'task_id': task_id,
            'dag_id': dag_id,
            'execution_date': execution_date.isoformat(),
            'duration': duration,
            'status': status,
            'timestamp': datetime.now().isoformat()
        })
    
    def record_dag_run(self, dag_id: str, execution_date: datetime,
                      status: str, task_count: int):
        """Record DAG execution metrics."""
        self.metrics['dag_runs'].append({
            'dag_id': dag_id,
            'execution_date': execution_date.isoformat(),
            'status': status,
            'task_count': task_count,
            'timestamp': datetime.now().isoformat()
        })
    
    def get_success_rate(self, dag_id: str = None, hours: int = 24) -> float:
        """Calculate success rate."""
        cutoff = datetime.now() - timedelta(hours=hours)
        
        runs = self.metrics['dag_runs']
        if dag_id:
            runs = [r for r in runs if r['dag_id'] == dag_id]
        
        runs = [r for r in runs 
                if datetime.fromisoformat(r['timestamp']) > cutoff]
        
        if not runs:
            return 100.0
        
        success_count = sum(1 for r in runs if r['status'] == 'success')
        return (success_count / len(runs)) * 100
    
    def get_average_duration(self, task_id: str = None) -> float:
        """Get average task duration."""
        runs = self.metrics['task_runs']
        if task_id:
            runs = [r for r in runs if r['task_id'] == task_id]
        
        if not runs:
            return 0.0
        
        return sum(r['duration'] for r in runs) / len(runs)
    
    def generate_report(self) -> Dict:
        """Generate monitoring report."""
        return {
            'total_task_runs': len(self.metrics['task_runs']),
            'total_dag_runs': len(self.metrics['dag_runs']),
            'success_rate_24h': self.get_success_rate(hours=24),
            'average_task_duration': self.get_average_duration(),
            'recent_failures': [f for f in self.metrics['failures'][-5:]]
        }


class AdvancedAirflowDemo:
    """Demonstrate advanced Airflow features."""
    
    def __init__(self):
        self.scheduler = AirflowScheduler()
        self.monitor = AirflowMonitor()
        self.execution_data = {}
        
    def simulate_etl_task(self, context: Dict) -> Dict:
        """Simulate ETL task with random success/failure."""
        task_id = context.get('task_id', 'unknown')
        
        # Simulate work
        time.sleep(0.1)
        
        # Random failure (10% chance)
        if random.random() < 0.1:
            raise Exception(f"Simulated failure in {task_id}")
        
        return {
            'task_id': task_id,
            'records_processed': random.randint(1000, 10000),
            'timestamp': datetime.now().isoformat()
        }
    
    def create_advanced_dag(self) -> Dict:
        """Create DAG with advanced features."""
        dag_id = 'advanced_etl_pipeline'
        
        tasks = {
            'extract': AdvancedAirflowTask(
                task_id='extract_data',
                python_callable=self.simulate_etl_task,
                retries=3,
                retry_delay=60,
                timeout=1800,
                email_on_failure=True,
                sla=timedelta(minutes=10)
            ),
            'transform': AdvancedAirflowTask(
                task_id='transform_data',
                python_callable=self.simulate_etl_task,
                dependencies=['extract_data'],
                retries=2,
                retry_delay=120,
                timeout=3600
            ),
            'validate': AdvancedAirflowTask(
                task_id='validate_data',
                python_callable=self.simulate_etl_task,
                dependencies=['transform_data'],
                retries=1,
                timeout=600
            ),
            'load': AdvancedAirflowTask(
                task_id='load_data',
                python_callable=self.simulate_etl_task,
                dependencies=['validate_data'],
                retries=3,
                retry_delay=300,
                timeout=1800
            ),
            'notify': AdvancedAirflowTask(
                task_id='send_notification',
                python_callable=self.simulate_etl_task,
                dependencies=['load_data'],
                retries=1
            )
        }
        
        return {
            'dag_id': dag_id,
            'tasks': tasks,
            'schedule_interval': '0 2 * * *',
            'catchup': False,
            'max_active_runs': 1
        }
    
    def demonstrate_retries(self) -> Dict:
        """Demonstrate retry mechanism."""
        logger.info("Demonstrating retry mechanism...")
        
        retry_results = []
        
        for attempt in range(4):
            try:
                # Simulate task that fails first 2 times
                if attempt < 2:
                    raise Exception(f"Simulated failure on attempt {attempt + 1}")
                
                retry_results.append({
                    'attempt': attempt + 1,
                    'status': 'success',
                    'message': 'Task succeeded'
                })
                break
                
            except Exception as e:
                retry_results.append({
                    'attempt': attempt + 1,
                    'status': 'failed',
                    'message': str(e),
                    'will_retry': attempt < 3
                })
                
                if attempt < 3:
                    logger.info(f"Retrying in 60 seconds...")
                    time.sleep(0.1)
        
        return {
            'retry_policy': {
                'retries': 3,
                'retry_delay': '60 seconds',
                'retry_exponential_backoff': True,
                'max_retry_delay': '10 minutes'
            },
            'retry_results': retry_results
        }
    
    def demonstrate_scheduling(self) -> Dict:
        """Demonstrate advanced scheduling."""
        logger.info("Demonstrating scheduling...")
        
        schedules = {
            'daily_2am': {
                'cron': '0 2 * * *',
                'description': 'Daily at 2:00 AM',
                'next_runs': ['2024-01-02 02:00', '2024-01-03 02:00', '2024-01-04 02:00']
            },
            'hourly': {
                'cron': '0 * * * *',
                'description': 'Every hour',
                'next_runs': ['02:00', '03:00', '04:00']
            },
            'every_6_hours': {
                'cron': '0 */6 * * *',
                'description': 'Every 6 hours',
                'next_runs': ['00:00', '06:00', '12:00', '18:00']
            },
            'weekly': {
                'cron': '0 0 * * 0',
                'description': 'Weekly on Sunday',
                'next_runs': ['2024-01-07', '2024-01-14', '2024-01-21']
            },
            'weekday_9am': {
                'cron': '0 9 * * 1-5',
                'description': 'Weekdays at 9:00 AM',
                'next_runs': ['Monday 9:00', 'Tuesday 9:00', 'Wednesday 9:00']
            }
        }
        
        return {
            'schedules': schedules,
            'features': {
                'catchup': 'Run missed intervals when DAG is enabled',
                'backfill': 'Run historical DAG runs',
                'depends_on_past': 'Wait for previous run to succeed',
                'wait_for_downstream': 'Wait for downstream tasks',
                'execution_timeout': 'Maximum time for task to run',
                'sla': 'Service Level Agreement for task completion'
            }
        }
    
    def demonstrate_monitoring(self) -> Dict:
        """Demonstrate monitoring capabilities."""
        logger.info("Demonstrating monitoring...")
        
        # Simulate some task runs
        for i in range(20):
            task_id = random.choice(['extract', 'transform', 'load'])
            status = random.choices(
                ['success', 'failed'],
                weights=[0.9, 0.1]
            )[0]
            
            self.monitor.record_task_run(
                task_id=task_id,
                dag_id='advanced_etl_pipeline',
                execution_date=datetime.now() - timedelta(hours=random.randint(1, 24)),
                duration=random.uniform(30, 300),
                status=status
            )
        
        # Generate report
        report = self.monitor.generate_report()
        
        return {
            'monitoring_metrics': {
                'task_duration': 'Time taken to execute task',
                'success_rate': 'Percentage of successful runs',
                'sla_misses': 'Tasks that exceeded SLA',
                'retry_count': 'Number of retries per task',
                'queue_time': 'Time spent waiting in queue'
            },
            'report': report,
            'alerting': {
                'email_on_failure': 'Send email when task fails',
                'email_on_retry': 'Send email when task retries',
                'sla_miss_email': 'Send email when SLA is missed',
                'pagerduty_integration': 'Alert via PagerDuty'
            }
        }
    
    def get_advanced_features(self) -> Dict:
        """Get advanced Airflow features."""
        return {
            'dynamic_dag_generation': 'Generate DAGs programmatically',
            'task_groups': 'Group related tasks visually',
            'subdags': 'Embed DAGs within DAGs',
            'triggers': 'Trigger DAGs from external events',
            'sensors': 'Wait for external conditions',
            'xcoms': 'Pass data between tasks',
            'connections': 'Manage external system connections',
            'variables': 'Store configuration values',
            'pools': 'Limit concurrent task execution',
            'queues': 'Route tasks to specific workers'
        }
    
    def run_demo(self) -> Dict:
        """Run complete advanced Airflow demonstration."""
        dag = self.create_advanced_dag()
        retries = self.demonstrate_retries()
        scheduling = self.demonstrate_scheduling()
        monitoring = self.demonstrate_monitoring()
        features = self.get_advanced_features()
        
        logger.info("Task 23 completed successfully!")
        
        return {
            'dag': dag,
            'retries': retries,
            'scheduling': scheduling,
            'monitoring': monitoring,
            'advanced_features': features
        }


def main():
    """Main execution for Task 23."""
    demo = AdvancedAirflowDemo()
    results = demo.run_demo()
    
    print("\n=== Advanced Airflow: Scheduling, Monitoring, Retries ===")
    
    print("\n1. DAG Structure:")
    print(f"  DAG ID: {results['dag']['dag_id']}")
    print(f"  Schedule: {results['dag']['schedule_interval']}")
    print(f"  Tasks: {list(results['dag']['tasks'].keys())}")
    
    print("\n2. Retry Policy:")
    print(f"  Retries: {results['retries']['retry_policy']['retries']}")
    print(f"  Retry Delay: {results['retries']['retry_policy']['retry_delay']}")
    
    print("\n3. Monitoring Report:")
    report = results['monitoring']['report']
    print(f"  Total Task Runs: {report['total_task_runs']}")
    print(f"  Success Rate (24h): {report['success_rate_24h']:.1f}%")
    print(f"  Avg Task Duration: {report['average_task_duration']:.1f}s")
    
    return results


if __name__ == "__main__":
    main()
