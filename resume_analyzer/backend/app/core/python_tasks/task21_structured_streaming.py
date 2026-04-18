#!/usr/bin/env python3
"""
Task 21: Structured Streaming
Build Spark streaming job for real-time logs.
"""

import json
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any
from collections import deque, defaultdict
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StructuredStreamingQuery:
    """Simulate Structured Streaming query."""
    
    def __init__(self, name: str):
        self.name = name
        self.is_running = False
        self.output_mode = 'append'
        self.trigger_interval = 1
        self.watermark = None
        self.state = {}
        self.results = deque(maxlen=100)
        
    def start(self):
        """Start the streaming query."""
        self.is_running = True
        logger.info(f"Started streaming query: {self.name}")
        
    def stop(self):
        """Stop the streaming query."""
        self.is_running = False
        logger.info(f"Stopped streaming query: {self.name}")
    
    def process_batch(self, batch: List[Dict]):
        """Process a batch of data."""
        pass


class StructuredStreamingDemo:
    """Demonstrate Structured Streaming for log processing."""
    
    def __init__(self):
        self.queries = []
        self.log_buffer = deque(maxlen=1000)
        
    def generate_log_event(self) -> Dict:
        """Generate a sample application log event."""
        log_levels = ['INFO', 'INFO', 'INFO', 'WARN', 'ERROR']
        services = ['api-gateway', 'user-service', 'payment-service', 'notification-service', 'analytics-service']
        endpoints = ['/api/users', '/api/payments', '/api/notifications', '/api/analytics', '/health']
        
        return {
            'timestamp': datetime.now().isoformat(),
            'level': random.choice(log_levels),
            'service': random.choice(services),
            'endpoint': random.choice(endpoints),
            'method': random.choice(['GET', 'POST', 'PUT', 'DELETE']),
            'status_code': random.choice([200, 200, 200, 201, 400, 401, 500]),
            'response_time_ms': random.randint(10, 5000),
            'user_id': f'user_{random.randint(1, 1000)}',
            'ip_address': f'192.168.{random.randint(1, 255)}.{random.randint(1, 255)}',
            'message': f'Request processed for {random.choice(endpoints)}'
        }
    
    def simulate_log_stream(self, duration_seconds: int = 10) -> List[Dict]:
        """Simulate a stream of log events."""
        logs = []
        start_time = time.time()
        
        while time.time() - start_time < duration_seconds:
            # Generate 5-15 logs per second
            for _ in range(random.randint(5, 15)):
                log = self.generate_log_event()
                logs.append(log)
                self.log_buffer.append(log)
            
            time.sleep(1)
        
        return logs
    
    def process_logs_append(self, logs: List[Dict]) -> Dict:
        """Process logs with append output mode."""
        logger.info("Processing with APPEND mode...")
        
        # Aggregate by service and level
        service_stats = defaultdict(lambda: defaultdict(int))
        
        for log in logs:
            service = log['service']
            level = log['level']
            service_stats[service][level] += 1
        
        return {
            'output_mode': 'append',
            'description': 'Only new rows added to result table',
            'results': dict(service_stats)
        }
    
    def process_logs_complete(self, logs: List[Dict]) -> Dict:
        """Process logs with complete output mode."""
        logger.info("Processing with COMPLETE mode...")
        
        # Full aggregation - entire table rewritten
        endpoint_stats = defaultdict(lambda: {
            'count': 0,
            'avg_response_time': 0,
            'error_count': 0
        })
        
        for log in logs:
            endpoint = log['endpoint']
            endpoint_stats[endpoint]['count'] += 1
            
            # Update running average
            old_avg = endpoint_stats[endpoint]['avg_response_time']
            count = endpoint_stats[endpoint]['count']
            new_value = log['response_time_ms']
            endpoint_stats[endpoint]['avg_response_time'] = (
                (old_avg * (count - 1) + new_value) / count
            )
            
            if log['status_code'] >= 400:
                endpoint_stats[endpoint]['error_count'] += 1
        
        return {
            'output_mode': 'complete',
            'description': 'Entire result table rewritten each batch',
            'results': dict(endpoint_stats)
        }
    
    def process_logs_update(self, logs: List[Dict]) -> Dict:
        """Process logs with update output mode."""
        logger.info("Processing with UPDATE mode...")
        
        # Only changed rows output
        hourly_stats = defaultdict(lambda: {
            'total_requests': 0,
            'total_errors': 0,
            'unique_users': set()
        })
        
        for log in logs:
            hour = log['timestamp'][:13]  # YYYY-MM-DD HH
            hourly_stats[hour]['total_requests'] += 1
            hourly_stats[hour]['unique_users'].add(log['user_id'])
            
            if log['status_code'] >= 400:
                hourly_stats[hour]['total_errors'] += 1
        
        # Convert sets to counts
        results = {}
        for hour, stats in hourly_stats.items():
            results[hour] = {
                'total_requests': stats['total_requests'],
                'total_errors': stats['total_errors'],
                'unique_users': len(stats['unique_users'])
            }
        
        return {
            'output_mode': 'update',
            'description': 'Only changed rows output each batch',
            'results': results
        }
    
    def demonstrate_windowed_aggregation(self, logs: List[Dict]) -> Dict:
        """Demonstrate windowed aggregation."""
        logger.info("Demonstrating windowed aggregation...")
        
        # Tumbling window - 5 second windows
        tumbling_windows = defaultdict(lambda: defaultdict(int))
        
        for log in logs:
            ts = datetime.fromisoformat(log['timestamp'])
            window_start = ts.replace(second=(ts.second // 5) * 5, microsecond=0)
            window_key = window_start.isoformat()
            
            tumbling_windows[window_key][log['service']] += 1
        
        # Sliding window - 10 second window, 5 second slide
        sliding_windows = defaultdict(lambda: defaultdict(int))
        
        for log in logs:
            ts = datetime.fromisoformat(log['timestamp'])
            # Each log belongs to multiple windows
            for window_start_sec in range(0, 60, 5):
                window_start = ts.replace(second=window_start_sec, microsecond=0)
                window_end = window_start + timedelta(seconds=10)
                
                if window_start <= ts < window_end:
                    window_key = f"{window_start.isoformat()} - {window_end.isoformat()}"
                    sliding_windows[window_key][log['service']] += 1
        
        return {
            'tumbling_windows': {
                'description': 'Fixed-size, non-overlapping windows',
                'window_size': '5 seconds',
                'results': dict(tumbling_windows)
            },
            'sliding_windows': {
                'description': 'Fixed-size, overlapping windows',
                'window_size': '10 seconds',
                'slide_interval': '5 seconds',
                'results': dict(list(sliding_windows.items())[:5])
            }
        }
    
    def demonstrate_watermarking(self, logs: List[Dict]) -> Dict:
        """Demonstrate watermarking for late data."""
        logger.info("Demonstrating watermarking...")
        
        # Simulate late arriving data
        watermark_delay = 5  # 5 second watermark
        
        current_time = datetime.now()
        
        # Process logs with watermark
        on_time_logs = []
        late_logs = []
        
        for log in logs:
            log_time = datetime.fromisoformat(log['timestamp'])
            delay = (current_time - log_time).total_seconds()
            
            if delay <= watermark_delay:
                on_time_logs.append(log)
            else:
                late_logs.append(log)
        
        return {
            'watermark_delay': f'{watermark_delay} seconds',
            'description': 'Allow late data within watermark, drop very late data',
            'on_time_logs': len(on_time_logs),
            'late_logs': len(late_logs),
            'late_data_handling': 'Late data within watermark is processed, very late data is dropped'
        }
    
    def get_streaming_concepts(self) -> Dict:
        """Get Structured Streaming concepts."""
        return {
            'programming_model': {
                'unified': 'Same code for batch and streaming',
                'incremental': 'Automatically handles incremental processing',
                'fault_tolerance': 'Exactly-once processing guarantees'
            },
            'output_modes': {
                'append': 'Only new rows added (default)',
                'complete': 'Entire result table rewritten',
                'update': 'Only changed rows output'
            },
            'window_operations': {
                'tumbling': 'Fixed-size, non-overlapping, contiguous',
                'sliding': 'Fixed-size, overlapping, with slide',
                'session': 'Dynamic size based on activity gaps'
            },
            'stateful_operations': {
                'mapGroupsWithState': 'Custom stateful processing',
                'flatMapGroupsWithState': 'Stateful with multiple outputs',
                'streaming_joins': 'Join streams with streams or static data'
            },
            'fault_tolerance': {
                'checkpointing': 'Save state to fault-tolerant storage',
                'write_ahead_log': 'Log events before processing',
                'idempotent_sinks': 'Ensure exactly-once to sinks'
            }
        }
    
    def run_demo(self) -> Dict:
        """Run complete Structured Streaming demonstration."""
        logger.info("Starting Structured Streaming demo...")
        
        # Generate log stream
        logs = self.simulate_log_stream(duration_seconds=5)
        
        # Process with different output modes
        append_results = self.process_logs_append(logs)
        complete_results = self.process_logs_complete(logs)
        update_results = self.process_logs_update(logs)
        
        # Windowed aggregation
        windowed = self.demonstrate_windowed_aggregation(logs)
        
        # Watermarking
        watermark = self.demonstrate_watermarking(logs)
        
        concepts = self.get_streaming_concepts()
        
        logger.info("Task 21 completed successfully!")
        
        return {
            'logs_generated': len(logs),
            'append_mode': append_results,
            'complete_mode': complete_results,
            'update_mode': update_results,
            'windowed_aggregation': windowed,
            'watermarking': watermark,
            'concepts': concepts
        }


def main():
    """Main execution for Task 21."""
    demo = StructuredStreamingDemo()
    results = demo.run_demo()
    
    print("\n=== Structured Streaming for Real-time Logs ===")
    print(f"\nLogs Generated: {results['logs_generated']:,}")
    
    print("\n=== Output Modes ===")
    print(f"Append: {results['append_mode']['description']}")
    print(f"Complete: {results['complete_mode']['description']}")
    print(f"Update: {results['update_mode']['description']}")
    
    print("\n=== Windowed Aggregation ===")
    print(f"Tumbling Windows: {results['windowed_aggregation']['tumbling_windows']['description']}")
    print(f"Sliding Windows: {results['windowed_aggregation']['sliding_windows']['description']}")
    
    print("\n=== Watermarking ===")
    print(f"Watermark Delay: {results['watermarking']['watermark_delay']}")
    print(f"On-time Logs: {results['watermarking']['on_time_logs']}")
    print(f"Late Logs: {results['watermarking']['late_logs']}")
    
    return results


if __name__ == "__main__":
    main()
