#!/usr/bin/env python3
"""
Task 19: Kafka Basics
Set up Kafka producer-consumer pipeline.
"""

import json
import logging
import time
import threading
from datetime import datetime
from typing import Dict, List, Any
from collections import deque
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaTopic:
    """Simulate Kafka Topic."""
    
    def __init__(self, name: str, partitions: int = 3, replication_factor: int = 1):
        self.name = name
        self.partitions = partitions
        self.replication_factor = replication_factor
        self.messages = {i: deque(maxlen=10000) for i in range(partitions)}
        self.consumer_offsets = {}
        
    def produce(self, message: Dict, key: str = None):
        """Produce a message to the topic."""
        # Determine partition based on key
        if key:
            partition = hash(key) % self.partitions
        else:
            partition = random.randint(0, self.partitions - 1)
        
        message_with_metadata = {
            'value': message,
            'key': key,
            'partition': partition,
            'offset': len(self.messages[partition]),
            'timestamp': datetime.now().isoformat()
        }
        
        self.messages[partition].append(message_with_metadata)
        return message_with_metadata
    
    def consume(self, consumer_group: str, partition: int = None, max_messages: int = 100):
        """Consume messages from the topic."""
        if consumer_group not in self.consumer_offsets:
            self.consumer_offsets[consumer_group] = {i: 0 for i in range(self.partitions)}
        
        consumed = []
        
        if partition is not None:
            partitions = [partition]
        else:
            partitions = range(self.partitions)
        
        for p in partitions:
            offset = self.consumer_offsets[consumer_group][p]
            messages = list(self.messages[p])[offset:offset + max_messages]
            consumed.extend(messages)
            self.consumer_offsets[consumer_group][p] = offset + len(messages)
        
        return consumed
    
    def get_lag(self, consumer_group: str) -> Dict:
        """Get consumer lag per partition."""
        if consumer_group not in self.consumer_offsets:
            return {i: len(self.messages[i]) for i in range(self.partitions)}
        
        lag = {}
        for p in range(self.partitions):
            current_offset = self.consumer_offsets[consumer_group][p]
            latest_offset = len(self.messages[p])
            lag[p] = latest_offset - current_offset
        
        return lag


class KafkaProducer:
    """Simulate Kafka Producer."""
    
    def __init__(self, bootstrap_servers: List[str] = None):
        self.bootstrap_servers = bootstrap_servers or ['localhost:9092']
        self.topics = {}
        self.metrics = {
            'messages_sent': 0,
            'bytes_sent': 0
        }
    
    def connect(self):
        """Connect to Kafka cluster."""
        logger.info(f"Producer connected to: {', '.join(self.bootstrap_servers)}")
    
    def send(self, topic: str, message: Dict, key: str = None) -> Dict:
        """Send a message to a topic."""
        if topic not in self.topics:
            self.topics[topic] = KafkaTopic(topic)
        
        result = self.topics[topic].produce(message, key)
        
        self.metrics['messages_sent'] += 1
        self.metrics['bytes_sent'] += len(json.dumps(message))
        
        return result
    
    def send_batch(self, topic: str, messages: List[Dict], key_func=None):
        """Send a batch of messages."""
        results = []
        for msg in messages:
            key = key_func(msg) if key_func else None
            results.append(self.send(topic, msg, key))
        return results
    
    def close(self):
        """Close producer connection."""
        logger.info("Producer closed")


class KafkaConsumer:
    """Simulate Kafka Consumer."""
    
    def __init__(self, topics: List[str], group_id: str, bootstrap_servers: List[str] = None):
        self.topics = topics
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers or ['localhost:9092']
        self.is_running = False
        self.message_handler = None
        self.metrics = {
            'messages_received': 0,
            'bytes_received': 0
        }
    
    def connect(self):
        """Connect to Kafka cluster."""
        logger.info(f"Consumer connected to: {', '.join(self.bootstrap_servers)}")
        logger.info(f"Subscribed to topics: {', '.join(self.topics)}")
    
    def subscribe(self, topics: List[str]):
        """Subscribe to topics."""
        self.topics = topics
        logger.info(f"Subscribed to: {', '.join(topics)}")
    
    def poll(self, timeout_ms: int = 1000, topic_manager: Dict = None) -> List[Dict]:
        """Poll for new messages."""
        messages = []
        
        for topic_name in self.topics:
            if topic_name in topic_manager:
                topic = topic_manager[topic_name]
                new_messages = topic.consume(self.group_id)
                messages.extend(new_messages)
                self.metrics['messages_received'] += len(new_messages)
                for msg in new_messages:
                    self.metrics['bytes_received'] += len(json.dumps(msg))
        
        return messages
    
    def consume(self, topic_manager: Dict, duration_seconds: int = 10):
        """Consume messages for a duration."""
        self.is_running = True
        self.connect()
        
        consumed_messages = []
        start_time = time.time()
        
        while self.is_running and (time.time() - start_time) < duration_seconds:
            messages = self.poll(topic_manager=topic_manager)
            if messages:
                consumed_messages.extend(messages)
                for msg in messages:
                    if self.message_handler:
                        self.message_handler(msg)
            time.sleep(0.5)
        
        return consumed_messages
    
    def close(self):
        """Close consumer connection."""
        self.is_running = False
        logger.info("Consumer closed")


class KafkaDemo:
    """Demonstrate Kafka producer-consumer pipeline."""
    
    def __init__(self):
        self.topic_manager = {}
        self.producer = None
        self.consumer = None
    
    def generate_resume_event(self) -> Dict:
        """Generate a resume-related event."""
        event_types = ['profile_created', 'profile_updated', 'application_submitted', 
                      'skill_added', 'experience_added']
        
        return {
            'event_type': random.choice(event_types),
            'candidate_id': f'candidate_{random.randint(1, 1000)}',
            'timestamp': datetime.now().isoformat(),
            'data': {
                'field': random.choice(['name', 'email', 'experience', 'skills']),
                'value': f'value_{random.randint(1000, 9999)}'
            }
        }
    
    def run_producer_demo(self) -> Dict:
        """Run producer demonstration."""
        logger.info("Starting Producer Demo...")
        
        producer = KafkaProducer(['localhost:9092'])
        producer.connect()
        
        topic_name = 'resume-events'
        
        # Create topic
        self.topic_manager[topic_name] = KafkaTopic(topic_name, partitions=3)
        
        produced_messages = []
        
        # Produce messages with keys for partitioning
        for i in range(50):
            event = self.generate_resume_event()
            # Use candidate_id as key for partitioning
            key = event['candidate_id']
            result = producer.send(topic_name, event, key=key)
            produced_messages.append(result)
            
            if (i + 1) % 10 == 0:
                logger.info(f"Produced {i + 1} messages")
        
        producer.close()
        
        # Analyze partition distribution
        topic = self.topic_manager[topic_name]
        partition_distribution = {p: len(topic.messages[p]) for p in range(topic.partitions)}
        
        return {
            'messages_produced': len(produced_messages),
            'partition_distribution': partition_distribution,
            'sample_messages': produced_messages[:3]
        }
    
    def run_consumer_demo(self) -> Dict:
        """Run consumer demonstration."""
        logger.info("Starting Consumer Demo...")
        
        consumer = KafkaConsumer(
            topics=['resume-events'],
            group_id='resume-processor-group',
            bootstrap_servers=['localhost:9092']
        )
        
        consumed_messages = consumer.consume(self.topic_manager, duration_seconds=2)
        
        # Check consumer lag
        topic = self.topic_manager['resume-events']
        lag = topic.get_lag('resume-processor-group')
        
        consumer.close()
        
        return {
            'messages_consumed': len(consumed_messages),
            'consumer_lag': lag,
            'sample_consumed': consumed_messages[:3] if consumed_messages else []
        }
    
    def run_multiple_consumers_demo(self) -> Dict:
        """Demonstrate consumer groups with multiple consumers."""
        logger.info("Starting Multiple Consumers Demo...")
        
        # Create consumer group with 3 consumers
        consumers = []
        for i in range(3):
            consumer = KafkaConsumer(
                topics=['resume-events'],
                group_id='load-balanced-group',
                bootstrap_servers=['localhost:9092']
            )
            consumers.append({
                'id': f'consumer-{i+1}',
                'instance': consumer
            })
        
        # Each consumer would get assigned partitions
        # In real Kafka, partition assignment is coordinated
        partition_assignments = {
            'consumer-1': [0],
            'consumer-2': [1],
            'consumer-3': [2]
        }
        
        return {
            'consumer_count': len(consumers),
            'partition_assignments': partition_assignments,
            'load_balancing': 'Each consumer in group gets assigned partitions'
        }
    
    def get_kafka_concepts(self) -> Dict:
        """Get Kafka concepts and architecture."""
        return {
            'core_concepts': {
                'topic': 'Category/feed name to which records are published',
                'partition': 'Ordered, immutable sequence of records',
                'offset': 'Unique identifier for each record within a partition',
                'producer': 'Publishes records to topics',
                'consumer': 'Subscribes to topics and processes records',
                'consumer_group': 'Group of consumers that share work'
            },
            'kafka_architecture': {
                'broker': 'Kafka server that stores and serves data',
                'cluster': 'Multiple brokers working together',
                'zookeeper': 'Coordinates Kafka brokers (being replaced by KRaft)',
                'replication': 'Copies of partitions across brokers for fault tolerance'
            },
            'delivery_semantics': {
                'at_most_once': 'Messages may be lost but never redelivered',
                'at_least_once': 'Messages never lost but may be redelivered',
                'exactly_once': 'Each message delivered exactly once (EOS)'
            },
            'configuration': {
                'producer': {
                    'acks': 'Number of acknowledgments required',
                    'retries': 'Number of retries for failed sends',
                    'batch.size': 'Batch size in bytes',
                    'linger.ms': 'Time to wait before sending batch'
                },
                'consumer': {
                    'group.id': 'Consumer group identifier',
                    'auto.offset.reset': 'Start from earliest or latest',
                    'enable.auto.commit': 'Automatically commit offsets'
                }
            }
        }
    
    def run_demo(self) -> Dict:
        """Run complete Kafka demonstration."""
        producer_results = self.run_producer_demo()
        consumer_results = self.run_consumer_demo()
        multi_consumer_results = self.run_multiple_consumers_demo()
        concepts = self.get_kafka_concepts()
        
        logger.info("Task 19 completed successfully!")
        
        return {
            'producer': producer_results,
            'consumer': consumer_results,
            'multiple_consumers': multi_consumer_results,
            'concepts': concepts
        }


def main():
    """Main execution for Task 19."""
    demo = KafkaDemo()
    results = demo.run_demo()
    
    print("\n=== Kafka Producer-Consumer Pipeline ===")
    
    print("\n1. Producer Results:")
    print(f"  Messages Produced: {results['producer']['messages_produced']}")
    print(f"  Partition Distribution: {results['producer']['partition_distribution']}")
    
    print("\n2. Consumer Results:")
    print(f"  Messages Consumed: {results['consumer']['messages_consumed']}")
    print(f"  Consumer Lag: {results['consumer']['consumer_lag']}")
    
    print("\n3. Core Concepts:")
    for concept, description in list(results['concepts']['core_concepts'].items())[:3]:
        print(f"  {concept}: {description}")
    
    return results


if __name__ == "__main__":
    main()
