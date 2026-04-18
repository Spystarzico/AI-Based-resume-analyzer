#!/usr/bin/env python3
"""
Task 18: Streaming Concepts
Simulate real-time data processing using streaming APIs.
"""

import json
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Callable
from collections import deque
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StreamingContext:
    """Simulate Spark Streaming Context."""
    
    def __init__(self, batch_duration: int = 1):
        self.batch_duration = batch_duration
        self.dstreams = []
        self.is_running = False
        self.start_time = None
        
    def start(self):
        """Start the streaming context."""
        self.is_running = True
        self.start_time = datetime.now()
        logger.info(f"StreamingContext started with batch duration {self.batch_duration}s")
    
    def stop(self):
        """Stop the streaming context."""
        self.is_running = False
        logger.info("StreamingContext stopped")
    
    def awaitTermination(self):
        """Wait for streaming to terminate."""
        while self.is_running:
            time.sleep(0.1)


class DStream:
    """Simulate Discretized Stream."""
    
    def __init__(self, source_func: Callable, ssc: StreamingContext):
        self.source_func = source_func
        self.ssc = ssc
        self.transformations = []
        self.output_ops = []
        self.batches = deque(maxlen=100)
        
    def map(self, func) -> 'DStream':
        """Apply map transformation."""
        new_dstream = DStream(self.source_func, self.ssc)
        new_dstream.transformations = self.transformations + [('map', func)]
        return new_dstream
    
    def filter(self, func) -> 'DStream':
        """Apply filter transformation."""
        new_dstream = DStream(self.source_func, self.ssc)
        new_dstream.transformations = self.transformations + [('filter', func)]
        return new_dstream
    
    def reduceByKey(self, func) -> 'DStream':
        """Apply reduceByKey transformation."""
        new_dstream = DStream(self.source_func, self.ssc)
        new_dstream.transformations = self.transformations + [('reduceByKey', func)]
        return new_dstream
    
    def window(self, window_duration: int, slide_duration: int = None) -> 'DStream':
        """Apply window operation."""
        new_dstream = DStream(self.source_func, self.ssc)
        new_dstream.transformations = self.transformations + [
            ('window', {'window': window_duration, 'slide': slide_duration})
        ]
        return new_dstream
    
    def updateStateByKey(self, func) -> 'DStream':
        """Apply stateful transformation."""
        new_dstream = DStream(self.source_func, self.ssc)
        new_dstream.transformations = self.transformations + [('updateStateByKey', func)]
        new_dstream.state = {}
        return new_dstream
    
    def foreachRDD(self, func):
        """Output operation - apply function to each RDD."""
        self.output_ops.append(('foreachRDD', func))
    
    def pprint(self, num: int = 10):
        """Print stream content."""
        self.output_ops.append(('pprint', num))
    
    def process_batch(self, data: List):
        """Process a batch of data through transformations."""
        result = data
        
        for op_name, op_func in self.transformations:
            if op_name == 'map':
                result = [op_func(r) for r in result]
            elif op_name == 'filter':
                result = [r for r in result if op_func(r)]
            elif op_name == 'reduceByKey':
                # Group by key and reduce
                grouped = {}
                for key, value in result:
                    if key not in grouped:
                        grouped[key] = []
                    grouped[key].append(value)
                result = [(k, op_func(v)) for k, v in grouped.items()]
            elif op_name == 'window':
                # Store in window
                self.batches.append(result)
                window_size = op_func['window']
                result = list(self.batches)[-window_size:]
                result = [item for batch in result for item in batch]
            elif op_name == 'updateStateByKey':
                # Update state
                new_state = {}
                for key, value in result:
                    old_state = getattr(self, 'state', {}).get(key)
                    new_state[key] = op_func(value, old_state)
                self.state = new_state
                result = list(new_state.items())
        
        return result


class StreamingSimulator:
    """Simulate real-time streaming data processing."""
    
    def __init__(self):
        self.processed_batches = []
        self.metrics = {
            'total_records': 0,
            'batches_processed': 0,
            'start_time': None
        }
    
    def generate_resume_event(self) -> Dict:
        """Generate a single resume-related event."""
        event_types = ['profile_view', 'application_submit', 'skill_add', 'experience_update']
        
        return {
            'event_id': f'evt_{random.randint(100000, 999999)}',
            'event_type': random.choice(event_types),
            'candidate_id': f'cand_{random.randint(1, 1000)}',
            'timestamp': datetime.now().isoformat(),
            'data': {
                'value': random.randint(1, 100),
                'metadata': json.dumps({'source': 'web', 'ip': f'192.168.{random.randint(1,255)}.{random.randint(1,255)}'})
            }
        }
    
    def simulate_stream_processing(self, duration_seconds: int = 10) -> Dict:
        """Simulate stream processing for a duration."""
        logger.info(f"Starting stream simulation for {duration_seconds} seconds...")
        
        ssc = StreamingContext(batch_duration=1)
        
        # Create input stream
        def event_source():
            return [self.generate_resume_event() for _ in range(random.randint(5, 15))]
        
        input_stream = DStream(event_source, ssc)
        
        # Apply transformations
        # 1. Extract event type and count
        event_counts = input_stream.map(lambda x: (x['event_type'], 1))
        
        # 2. Reduce by key (event type)
        aggregated = event_counts.reduceByKey(lambda a, b: a + b)
        
        # 3. Stateful counting
        def update_count(new_values, running_count):
            if running_count is None:
                running_count = 0
            return running_count + sum(new_values) if isinstance(new_values, list) else running_count + new_values
        
        stateful_counts = aggregated.updateStateByKey(update_count)
        
        # Store results
        results = []
        stateful_counts.output_ops.append(('store', lambda x: results.append(x)))
        
        # Start processing
        ssc.start()
        self.metrics['start_time'] = datetime.now()
        
        batch_results = []
        for batch_num in range(duration_seconds):
            if not ssc.is_running:
                break
            
            # Generate batch
            batch_data = event_source()
            
            # Process batch
            processed = stateful_counts.process_batch([(e['event_type'], 1) for e in batch_data])
            
            batch_results.append({
                'batch': batch_num + 1,
                'records': len(batch_data),
                'aggregated': processed
            })
            
            self.metrics['total_records'] += len(batch_data)
            self.metrics['batches_processed'] += 1
            
            logger.info(f"Batch {batch_num + 1}: {len(batch_data)} events, State: {dict(processed)}")
            
            time.sleep(1)
        
        ssc.stop()
        
        return {
            'batch_results': batch_results,
            'final_state': dict(processed) if processed else {},
            'metrics': self.metrics
        }
    
    def demonstrate_window_operations(self) -> Dict:
        """Demonstrate windowed operations."""
        logger.info("Demonstrating window operations...")
        
        # Simulate 1-minute window with 10-second slides
        window_duration = 6  # 6 batches = 6 seconds for demo
        slide_duration = 2   # 2 batches = 2 seconds for demo
        
        # Generate events with timestamps
        events = []
        base_time = datetime.now()
        
        for i in range(60):  # 60 events over "60 seconds"
            events.append({
                'timestamp': base_time + timedelta(seconds=i),
                'value': random.randint(1, 100),
                'category': random.choice(['A', 'B', 'C'])
            })
        
        # Apply tumbling window (non-overlapping)
        tumbling_windows = {}
        for event in events:
            window_key = (event['timestamp'].second // 10) * 10
            if window_key not in tumbling_windows:
                tumbling_windows[window_key] = []
            tumbling_windows[window_key].append(event)
        
        # Apply sliding window (overlapping)
        sliding_windows = {}
        for event in events:
            for window_start in range(0, 60, 5):
                window_end = window_start + 10
                if window_start <= event['timestamp'].second < window_end:
                    key = f"{window_start}-{window_end}"
                    if key not in sliding_windows:
                        sliding_windows[key] = []
                    sliding_windows[key].append(event)
        
        return {
            'tumbling_windows': {
                'description': 'Non-overlapping fixed-size windows',
                'windows': {k: len(v) for k, v in list(tumbling_windows.items())[:5]}
            },
            'sliding_windows': {
                'description': 'Overlapping windows with slide interval',
                'windows': {k: len(v) for k, v in list(sliding_windows.items())[:5]}
            },
            'window_types': {
                'tumbling': 'Fixed-size, non-overlapping, contiguous',
                'sliding': 'Fixed-size, overlapping, with slide interval',
                'session': 'Dynamic size, based on activity gaps'
            }
        }
    
    def get_streaming_concepts(self) -> Dict:
        """Get streaming data concepts."""
        return {
            'streaming_architecture': {
                'components': [
                    'Data Source (Kafka, Kinesis, etc.)',
                    'Streaming Engine (Spark Streaming, Flink)',
                    'Processing Logic (Transformations)',
                    'Sink (Database, Dashboard, etc.)'
                ],
                'processing_modes': {
                    'at_most_once': 'Messages may be lost (fastest)',
                    'at_least_once': 'Messages may be duplicated',
                    'exactly_once': 'Messages processed exactly once (hardest)'
                }
            },
            'latency_types': {
                'processing_latency': 'Time to process one event',
                'end_to_end_latency': 'Time from source to sink',
                'window_latency': 'Time to close a window'
            },
            'state_management': {
                'stateless': 'Each event processed independently',
                'stateful': 'Maintain state across events',
                'state_stores': ['In-memory', 'RocksDB', 'External DB']
            },
            'fault_tolerance': {
                'checkpointing': 'Save state periodically',
                'write_ahead_log': 'Log events before processing',
                'replication': 'Replicate state across nodes'
            }
        }
    
    def run_demo(self) -> Dict:
        """Run complete streaming demonstration."""
        # Simulate stream processing
        stream_results = self.simulate_stream_processing(duration_seconds=5)
        
        # Demonstrate window operations
        window_results = self.demonstrate_window_operations()
        
        # Get concepts
        concepts = self.get_streaming_concepts()
        
        logger.info("Task 18 completed successfully!")
        
        return {
            'stream_processing': stream_results,
            'window_operations': window_results,
            'concepts': concepts
        }


def main():
    """Main execution for Task 18."""
    simulator = StreamingSimulator()
    results = simulator.run_demo()
    
    print("\n=== Streaming Concepts ===")
    print("\nProcessing Modes:")
    for mode, desc in results['concepts']['streaming_architecture']['processing_modes'].items():
        print(f"  {mode}: {desc}")
    
    print("\n=== Stream Processing Results ===")
    print(f"Total Records: {results['stream_processing']['metrics']['total_records']:,}")
    print(f"Batches Processed: {results['stream_processing']['metrics']['batches_processed']}")
    
    print("\n=== Final State ===")
    for event_type, count in results['stream_processing']['final_state'].items():
        print(f"  {event_type}: {count}")
    
    return results


if __name__ == "__main__":
    main()
