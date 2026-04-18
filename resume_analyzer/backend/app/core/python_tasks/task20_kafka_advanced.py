#!/usr/bin/env python3
"""
Task 20: Kafka Advanced
Implement partitioning and offset management.
"""

import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Any
from collections import defaultdict
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AdvancedKafkaPartitioner:
    """Advanced Kafka partitioning strategies."""
    
    def __init__(self, num_partitions: int = 6):
        self.num_partitions = num_partitions
    
    def default_partitioner(self, key: str, value: Dict) -> int:
        """Default hash-based partitioner."""
        if key is None:
            return random.randint(0, self.num_partitions - 1)
        return hash(key) % self.num_partitions
    
    def round_robin_partitioner(self, key: str, value: Dict, sequence: int) -> int:
        """Round-robin partitioner for even distribution."""
        return sequence % self.num_partitions
    
    def custom_partitioner(self, key: str, value: Dict) -> int:
        """Custom partitioner based on message content."""
        # Route based on event type
        event_type = value.get('event_type', 'default')
        
        partition_mapping = {
            'critical': 0,  # Dedicated partition for critical events
            'high_priority': [1, 2],
            'normal': [3, 4],
            'low_priority': [5]
        }
        
        assigned = partition_mapping.get(event_type, list(range(self.num_partitions)))
        
        if isinstance(assigned, list):
            # Use key hash to select from allowed partitions
            return assigned[hash(key) % len(assigned)]
        return assigned
    
    def geographic_partitioner(self, key: str, value: Dict) -> int:
        """Partition based on geographic region."""
        region = value.get('region', 'unknown')
        
        region_partitions = {
            'north_america': [0, 1],
            'europe': [2, 3],
            'asia': [4, 5]
        }
        
        partitions = region_partitions.get(region, list(range(self.num_partitions)))
        return partitions[hash(key) % len(partitions)]


class OffsetManager:
    """Manage Kafka consumer offsets."""
    
    def __init__(self):
        self.offsets = defaultdict(lambda: defaultdict(int))
        self.committed_offsets = defaultdict(lambda: defaultdict(int))
        self.offset_history = []
    
    def get_current_offset(self, group_id: str, topic: str, partition: int) -> int:
        """Get current offset for a consumer group."""
        return self.offsets[group_id][f"{topic}:{partition}"]
    
    def update_offset(self, group_id: str, topic: str, partition: int, offset: int):
        """Update current offset."""
        key = f"{topic}:{partition}"
        self.offsets[group_id][key] = offset
        self.offset_history.append({
            'group_id': group_id,
            'topic': topic,
            'partition': partition,
            'offset': offset,
            'timestamp': datetime.now().isoformat(),
            'action': 'update'
        })
    
    def commit_offset(self, group_id: str, topic: str, partition: int):
        """Commit offset to persistent storage."""
        key = f"{topic}:{partition}"
        self.committed_offsets[group_id][key] = self.offsets[group_id][key]
        
        logger.info(f"Committed offset for {group_id}/{topic}/{partition}: {self.offsets[group_id][key]}")
        
        self.offset_history.append({
            'group_id': group_id,
            'topic': topic,
            'partition': partition,
            'offset': self.offsets[group_id][key],
            'timestamp': datetime.now().isoformat(),
            'action': 'commit'
        })
    
    def seek_to_offset(self, group_id: str, topic: str, partition: int, offset: int):
        """Seek to a specific offset."""
        key = f"{topic}:{partition}"
        self.offsets[group_id][key] = offset
        
        logger.info(f"Seeked {group_id}/{topic}/{partition} to offset {offset}")
        
        self.offset_history.append({
            'group_id': group_id,
            'topic': topic,
            'partition': partition,
            'offset': offset,
            'timestamp': datetime.now().isoformat(),
            'action': 'seek'
        })
    
    def seek_to_beginning(self, group_id: str, topic: str, partition: int):
        """Seek to beginning of partition."""
        self.seek_to_offset(group_id, topic, partition, 0)
    
    def seek_to_end(self, group_id: str, topic: str, partition: int, latest_offset: int):
        """Seek to end of partition."""
        self.seek_to_offset(group_id, topic, partition, latest_offset)
    
    def get_lag(self, group_id: str, topic: str, partition: int, latest_offset: int) -> int:
        """Calculate consumer lag."""
        current = self.get_current_offset(group_id, topic, partition)
        return latest_offset - current
    
    def get_commit_history(self) -> List[Dict]:
        """Get offset commit history."""
        return self.offset_history


class AdvancedKafkaDemo:
    """Demonstrate advanced Kafka features."""
    
    def __init__(self):
        self.partitioner = AdvancedKafkaPartitioner(num_partitions=6)
        self.offset_manager = OffsetManager()
        self.messages = defaultdict(list)
        
    def generate_events(self, n: int = 100) -> List[Dict]:
        """Generate events with different priorities."""
        events = []
        
        for i in range(n):
            event = {
                'event_id': f'evt_{i}',
                'event_type': random.choice([
                    'critical', 'critical',
                    'high_priority', 'high_priority', 'high_priority',
                    'normal', 'normal', 'normal', 'normal', 'normal',
                    'low_priority', 'low_priority'
                ]),
                'candidate_id': f'cand_{random.randint(1, 100)}',
                'region': random.choice(['north_america', 'europe', 'asia']),
                'timestamp': datetime.now().isoformat(),
                'data': {'value': random.randint(1, 100)}
            }
            events.append(event)
        
        return events
    
    def demonstrate_partitioning(self) -> Dict:
        """Demonstrate different partitioning strategies."""
        logger.info("Demonstrating partitioning strategies...")
        
        events = self.generate_events(60)
        
        results = {}
        
        # 1. Default partitioning
        default_dist = defaultdict(int)
        for i, event in enumerate(events):
            partition = self.partitioner.default_partitioner(event['candidate_id'], event)
            default_dist[partition] += 1
        
        results['default'] = {
            'description': 'Hash-based partitioning on key',
            'distribution': dict(default_dist)
        }
        
        # 2. Round-robin partitioning
        rr_dist = defaultdict(int)
        for i, event in enumerate(events):
            partition = self.partitioner.round_robin_partitioner(None, event, i)
            rr_dist[partition] += 1
        
        results['round_robin'] = {
            'description': 'Even distribution across partitions',
            'distribution': dict(rr_dist)
        }
        
        # 3. Custom partitioning (by event type)
        custom_dist = defaultdict(int)
        for event in events:
            partition = self.partitioner.custom_partitioner(event['candidate_id'], event)
            custom_dist[partition] += 1
        
        results['custom'] = {
            'description': 'Priority-based partitioning',
            'distribution': dict(custom_dist),
            'note': 'Critical events (0), High (1-2), Normal (3-4), Low (5)'
        }
        
        # 4. Geographic partitioning
        geo_dist = defaultdict(int)
        for event in events:
            partition = self.partitioner.geographic_partitioner(event['candidate_id'], event)
            geo_dist[partition] += 1
        
        results['geographic'] = {
            'description': 'Region-based partitioning',
            'distribution': dict(geo_dist),
            'mapping': 'NA (0-1), EU (2-3), Asia (4-5)'
        }
        
        return results
    
    def demonstrate_offset_management(self) -> Dict:
        """Demonstrate offset management."""
        logger.info("Demonstrating offset management...")
        
        group_id = 'resume-processor'
        topic = 'resume-events'
        partition = 0
        
        # Simulate message processing
        for offset in range(10):
            # Process message
            time.sleep(0.1)
            
            # Update offset
            self.offset_manager.update_offset(group_id, topic, partition, offset + 1)
            
            # Commit every 3 messages (simulate auto-commit interval)
            if (offset + 1) % 3 == 0:
                self.offset_manager.commit_offset(group_id, topic, partition)
        
        # Simulate reprocessing scenario
        logger.info("Simulating reprocessing from offset 5...")
        self.offset_manager.seek_to_offset(group_id, topic, partition, 5)
        
        # Get lag
        lag = self.offset_manager.get_lag(group_id, topic, partition, latest_offset=100)
        
        return {
            'current_offset': self.offset_manager.get_current_offset(group_id, topic, partition),
            'committed_offset': self.offset_manager.committed_offsets[group_id][f"{topic}:{partition}"],
            'consumer_lag': lag,
            'commit_history': self.offset_manager.get_commit_history()
        }
    
    def demonstrate_consumer_rebalancing(self) -> Dict:
        """Demonstrate consumer group rebalancing."""
        logger.info("Demonstrating consumer rebalancing...")
        
        # Initial state: 3 consumers, 6 partitions
        initial_assignment = {
            'consumer-1': [0, 1],
            'consumer-2': [2, 3],
            'consumer-3': [4, 5]
        }
        
        # After consumer-2 leaves: rebalance
        after_leave = {
            'consumer-1': [0, 1, 2],
            'consumer-3': [3, 4, 5]
        }
        
        # After new consumer joins: rebalance again
        after_join = {
            'consumer-1': [0, 1],
            'consumer-3': [2, 3],
            'consumer-4': [4, 5]
        }
        
        return {
            'initial_assignment': initial_assignment,
            'after_consumer_leave': after_leave,
            'after_new_consumer_join': after_join,
            'rebalancing_strategies': {
                'range': 'Assign contiguous partition ranges',
                'roundrobin': 'Distribute partitions evenly one by one',
                'sticky': 'Preserve existing assignments when possible'
            }
        }
    
    def get_advanced_concepts(self) -> Dict:
        """Get advanced Kafka concepts."""
        return {
            'partitioning_strategies': {
                'default': 'Hash of key modulo number of partitions',
                'round_robin': 'Even distribution when no key',
                'custom': 'User-defined logic for partition selection',
                'semantic': 'Preserve ordering for related messages'
            },
            'offset_management': {
                'auto_commit': 'Automatically commit offsets periodically',
                'manual_commit': 'Application controls when to commit',
                'store_externally': 'Store offsets in external system',
                'exactly_once': 'Transaction-based offset commits'
            },
            'consumer_groups': {
                'partition_assignment': 'How partitions are assigned to consumers',
                'rebalancing': 'Redistribute partitions when consumers join/leave',
                'static_membership': 'Preserve assignment across restarts'
            },
            'performance_tuning': {
                'batch_size': 'Number of messages per request',
                'linger_ms': 'Time to wait for batching',
                'compression': 'Compress messages (gzip, snappy, lz4)',
                'fetch_min_bytes': 'Minimum data to return'
            }
        }
    
    def run_demo(self) -> Dict:
        """Run complete advanced Kafka demonstration."""
        partitioning = self.demonstrate_partitioning()
        offset_mgmt = self.demonstrate_offset_management()
        rebalancing = self.demonstrate_consumer_rebalancing()
        concepts = self.get_advanced_concepts()
        
        logger.info("Task 20 completed successfully!")
        
        return {
            'partitioning': partitioning,
            'offset_management': offset_mgmt,
            'rebalancing': rebalancing,
            'concepts': concepts
        }


def main():
    """Main execution for Task 20."""
    demo = AdvancedKafkaDemo()
    results = demo.run_demo()
    
    print("\n=== Advanced Kafka: Partitioning & Offset Management ===")
    
    print("\n1. Partitioning Strategies:")
    for strategy, info in results['partitioning'].items():
        print(f"\n  {strategy}:")
        print(f"    Description: {info['description']}")
        print(f"    Distribution: {info['distribution']}")
    
    print("\n2. Offset Management:")
    print(f"  Current Offset: {results['offset_management']['current_offset']}")
    print(f"  Committed Offset: {results['offset_management']['committed_offset']}")
    print(f"  Consumer Lag: {results['offset_management']['consumer_lag']}")
    
    print("\n3. Consumer Rebalancing:")
    print(f"  Initial: {results['rebalancing']['initial_assignment']}")
    print(f"  After Leave: {results['rebalancing']['after_consumer_leave']}")
    
    return results


if __name__ == "__main__":
    main()
