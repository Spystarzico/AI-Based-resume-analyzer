#!/usr/bin/env python3
"""
Task 17: PySpark Advanced
Optimize Spark jobs using partitioning and caching.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any
import random
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OptimizedSparkJob:
    """Advanced Spark job with optimization techniques."""
    
    def __init__(self, app_name: str):
        self.app_name = app_name
        self.cached_rdds = {}
        self.execution_stats = {
            'partitions': {},
            'cache_hits': 0,
            'shuffle_operations': 0
        }
    
    def generate_large_dataset(self, n: int = 10000) -> List[Dict]:
        """Generate large dataset for optimization testing."""
        first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Chris', 'Jessica']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']
        locations = ['New York', 'Los Angeles', 'Chicago', 'Seattle', 'Austin', 'Boston', 'Denver']
        departments = ['Engineering', 'Data Science', 'DevOps', 'Product', 'Sales', 'Marketing']
        skills_pool = ['Python', 'Java', 'SQL', 'Spark', 'AWS', 'Azure', 'Docker', 'Kubernetes', 'ML']
        
        data = []
        for i in range(n):
            data.append({
                'id': i + 1,
                'name': f'{random.choice(first_names)} {random.choice(last_names)}',
                'location': random.choice(locations),
                'department': random.choice(departments),
                'experience_years': random.randint(0, 20),
                'salary': random.randint(50000, 200000),
                'skills': random.sample(skills_pool, random.randint(2, 5)),
                'join_date': (datetime.now() - timedelta(days=random.randint(1, 3650))).strftime('%Y-%m-%d'),
                'is_active': random.choice([True, True, True, False])
            })
        
        return data
    
    def demonstrate_partitioning(self, data: List[Dict]) -> Dict:
        """Demonstrate partitioning strategies."""
        logger.info("Demonstrating partitioning strategies...")
        
        results = {}
        
        # 1. Default Hash Partitioning
        # Distributes data evenly based on hash of key
        hash_partitioned = self._hash_partition(data, 'department', num_partitions=8)
        results['hash_partitioning'] = {
            'description': 'Distributes data based on hash of key',
            'partitions': len(hash_partitioned),
            'distribution': {f'p{i}': len(p) for i, p in enumerate(hash_partitioned)}
        }
        
        # 2. Range Partitioning
        # Distributes data based on ranges of key
        range_partitioned = self._range_partition(data, 'salary', num_partitions=4)
        results['range_partitioning'] = {
            'description': 'Distributes data based on value ranges',
            'partitions': len(range_partitioned),
            'ranges': ['0-75K', '75K-100K', '100K-150K', '150K+'],
            'distribution': {f'p{i}': len(p) for i, p in enumerate(range_partitioned)}
        }
        
        # 3. Custom Partitioning (by location)
        location_partitions = self._custom_partition(data, 'location')
        results['custom_partitioning'] = {
            'description': 'Custom partitioning by location',
            'partitions': len(location_partitions),
            'distribution': {loc: len(rows) for loc, rows in location_partitions.items()}
        }
        
        return results
    
    def _hash_partition(self, data: List[Dict], key: str, num_partitions: int) -> List[List]:
        """Hash partitioning implementation."""
        partitions = [[] for _ in range(num_partitions)]
        
        for row in data:
            key_value = str(row.get(key, ''))
            hash_value = int(hashlib.md5(key_value.encode()).hexdigest(), 16)
            partition_id = hash_value % num_partitions
            partitions[partition_id].append(row)
        
        return partitions
    
    def _range_partition(self, data: List[Dict], key: str, num_partitions: int) -> List[List]:
        """Range partitioning implementation."""
        values = [r.get(key, 0) for r in data]
        min_val, max_val = min(values), max(values)
        range_size = (max_val - min_val) / num_partitions
        
        partitions = [[] for _ in range(num_partitions)]
        
        for row in data:
            value = row.get(key, 0)
            partition_id = min(int((value - min_val) / range_size), num_partitions - 1)
            partitions[partition_id].append(row)
        
        return partitions
    
    def _custom_partition(self, data: List[Dict], key: str) -> Dict:
        """Custom partitioning by key value."""
        partitions = {}
        
        for row in data:
            key_value = row.get(key, 'unknown')
            if key_value not in partitions:
                partitions[key_value] = []
            partitions[key_value].append(row)
        
        return partitions
    
    def demonstrate_caching(self, data: List[Dict]) -> Dict:
        """Demonstrate caching strategies."""
        logger.info("Demonstrating caching strategies...")
        
        results = {}
        
        # Simulate expensive computation
        def expensive_computation(row):
            # Simulate complex processing
            result = row.copy()
            result['computed_score'] = (
                row['salary'] * 0.5 +
                row['experience_years'] * 5000 +
                len(row['skills']) * 10000
            )
            return result
        
        # Without caching - compute multiple times
        start = datetime.now()
        computed_once = [expensive_computation(r) for r in data[:100]]
        computed_twice = [expensive_computation(r) for r in data[:100]]
        without_cache_time = (datetime.now() - start).total_seconds()
        
        # With caching - compute once, reuse
        start = datetime.now()
        cache_key = 'computed_data'
        if cache_key not in self.cached_rdds:
            self.cached_rdds[cache_key] = [expensive_computation(r) for r in data[:100]]
            logger.info(f"Cached data with key: {cache_key}")
        
        cached_data = self.cached_rdds[cache_key]
        reused_data = self.cached_rdds[cache_key]  # Reuse from cache
        with_cache_time = (datetime.now() - start).total_seconds()
        
        results['caching_comparison'] = {
            'without_cache': {
                'time_seconds': without_cache_time,
                'description': 'Computed twice, no caching'
            },
            'with_cache': {
                'time_seconds': with_cache_time,
                'description': 'Computed once, reused from cache',
                'speedup': without_cache_time / with_cache_time if with_cache_time > 0 else float('inf')
            }
        }
        
        # Cache persistence levels
        results['persistence_levels'] = {
            'MEMORY_ONLY': 'Store in memory only (default)',
            'MEMORY_AND_DISK': 'Store in memory, spill to disk if needed',
            'MEMORY_ONLY_SER': 'Store serialized in memory (more space efficient)',
            'DISK_ONLY': 'Store on disk only',
            'OFF_HEAP': 'Store in off-heap memory'
        }
        
        return results
    
    def demonstrate_broadcast_join(self, large_data: List[Dict], small_data: List[Dict]) -> Dict:
        """Demonstrate broadcast join optimization."""
        logger.info("Demonstrating broadcast join...")
        
        # Regular shuffle join - expensive
        start = datetime.now()
        shuffle_result = self._shuffle_join(large_data, small_data, 'department')
        shuffle_time = (datetime.now() - start).total_seconds()
        
        # Broadcast join - optimized
        start = datetime.now()
        broadcast_result = self._broadcast_join(large_data, small_data, 'department')
        broadcast_time = (datetime.now() - start).total_seconds()
        
        return {
            'shuffle_join': {
                'time_seconds': shuffle_time,
                'description': 'Shuffle data across network',
                'records': len(shuffle_result)
            },
            'broadcast_join': {
                'time_seconds': broadcast_time,
                'description': 'Broadcast small table to all nodes',
                'records': len(broadcast_result),
                'speedup': shuffle_time / broadcast_time if broadcast_time > 0 else float('inf')
            },
            'when_to_use': 'Use broadcast join when one table is small (< 10MB)'
        }
    
    def _shuffle_join(self, left: List[Dict], right: List[Dict], key: str) -> List[Dict]:
        """Simulate shuffle join."""
        result = []
        for l_row in left:
            for r_row in right:
                if l_row.get(key) == r_row.get(key):
                    result.append({**l_row, **r_row})
        return result
    
    def _broadcast_join(self, large: List[Dict], small: List[Dict], key: str) -> List[Dict]:
        """Simulate broadcast join - broadcast small table."""
        # Build hash table from small table
        broadcast_table = {}
        for row in small:
            key_value = row.get(key)
            if key_value not in broadcast_table:
                broadcast_table[key_value] = []
            broadcast_table[key_value].append(row)
        
        # Probe with large table
        result = []
        for row in large:
            key_value = row.get(key)
            if key_value in broadcast_table:
                for small_row in broadcast_table[key_value]:
                    result.append({**row, **small_row})
        
        return result
    
    def demonstrate_shuffle_optimization(self, data: List[Dict]) -> Dict:
        """Demonstrate shuffle optimization."""
        logger.info("Demonstrating shuffle optimization...")
        
        results = {}
        
        # Reduce shuffle by pre-aggregating
        start = datetime.now()
        
        # Without optimization - multiple shuffles
        map_side_result = self._map_side_combine(data)
        
        map_side_time = (datetime.now() - start).total_seconds()
        
        results['map_side_combine'] = {
            'description': 'Pre-aggregate on map side before shuffle',
            'time_seconds': map_side_time,
            'benefits': [
                'Reduces amount of data shuffled',
                'Less network traffic',
                'Faster aggregation'
            ]
        }
        
        # Salting for skewed data
        results['salting'] = {
            'description': 'Add random prefix to skewed keys',
            'use_case': 'When data is skewed (some keys have many more records)',
            'implementation': 'Add random salt (0-9) to key, aggregate, then remove salt'
        }
        
        return results
    
    def _map_side_combine(self, data: List[Dict]) -> Dict:
        """Map-side combine to reduce shuffle."""
        # Local aggregation before shuffle
        local_combined = {}
        for row in data:
            dept = row.get('department')
            if dept not in local_combined:
                local_combined[dept] = {'count': 0, 'total_salary': 0}
            local_combined[dept]['count'] += 1
            local_combined[dept]['total_salary'] += row.get('salary', 0)
        
        return local_combined
    
    def get_optimization_tips(self) -> Dict:
        """Get Spark optimization tips."""
        return {
            'partitioning': {
                'tips': [
                    'Use appropriate number of partitions (2-4 per CPU core)',
                    'Repartition before expensive joins',
                    'Use coalesce to reduce partitions before writing',
                    'Consider data skew when partitioning'
                ]
            },
            'caching': {
                'tips': [
                    'Cache RDDs that are used multiple times',
                    'Unpersist when cache is no longer needed',
                    'Choose right persistence level for your use case',
                    'Monitor storage levels in Spark UI'
                ]
            },
            'shuffle_optimization': {
                'tips': [
                    'Minimize shuffle operations',
                    'Use broadcast joins for small tables',
                    'Enable map-side combine for aggregations',
                    'Use salting for skewed data',
                    'Tune spark.sql.shuffle.partitions'
                ]
            },
            'serialization': {
                'tips': [
                    'Use Kryo serialization instead of Java',
                    'Register custom classes with Kryo',
                    'Avoid nested structures in RDDs'
                ]
            }
        }
    
    def run_demo(self) -> Dict:
        """Run complete optimization demonstration."""
        # Generate data
        large_data = self.generate_large_dataset(5000)
        small_data = [{'department': d, 'budget': random.randint(1000000, 5000000)}
                      for d in set(r['department'] for r in large_data)]
        
        # Run demonstrations
        partitioning = self.demonstrate_partitioning(large_data)
        caching = self.demonstrate_caching(large_data)
        broadcast = self.demonstrate_broadcast_join(large_data, small_data)
        shuffle_opt = self.demonstrate_shuffle_optimization(large_data)
        tips = self.get_optimization_tips()
        
        logger.info("Task 17 completed successfully!")
        
        return {
            'partitioning': partitioning,
            'caching': caching,
            'broadcast_join': broadcast,
            'shuffle_optimization': shuffle_opt,
            'optimization_tips': tips
        }


def main():
    """Main execution for Task 17."""
    from datetime import timedelta
    
    job = OptimizedSparkJob("OptimizationDemo")
    results = job.run_demo()
    
    print("\n=== Spark Optimization Results ===")
    
    print("\n1. Partitioning Distribution:")
    for ptype, info in results['partitioning'].items():
        print(f"\n  {ptype}:")
        if 'distribution' in info:
            for k, v in list(info['distribution'].items())[:3]:
                print(f"    {k}: {v} records")
    
    print("\n2. Caching Speedup:")
    speedup = results['caching']['caching_comparison']['with_cache'].get('speedup', 0)
    print(f"  Cache speedup: {speedup:.2f}x")
    
    print("\n3. Broadcast Join:")
    broadcast_speedup = results['broadcast_join']['broadcast_join'].get('speedup', 0)
    print(f"  Broadcast join speedup: {broadcast_speedup:.2f}x")
    
    return results


if __name__ == "__main__":
    main()
