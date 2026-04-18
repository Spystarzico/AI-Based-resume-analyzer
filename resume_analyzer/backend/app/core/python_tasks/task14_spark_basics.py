#!/usr/bin/env python3
"""
Task 14: Spark Basics
Create Spark job to process large dataset.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any
import random
import hashlib
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkContext:
    """Simulate Apache Spark Context."""
    
    def __init__(self, app_name: str, master: str = "local"):
        self.app_name = app_name
        self.master = master
        self.start_time = datetime.now()
        self.rdds = []
        logger.info(f"Spark Context initialized: {app_name} on {master}")
    
    def parallelize(self, data: List, num_partitions: int = 4):
        """Create an RDD from a local collection."""
        return RDD(data, self, num_partitions)
    
    def textFile(self, path: str, min_partitions: int = 4):
        """Read a text file into an RDD."""
        try:
            with open(path, 'r') as f:
                lines = f.readlines()
            return RDD(lines, self, min_partitions)
        except FileNotFoundError:
            logger.error(f"File not found: {path}")
            return RDD([], self, min_partitions)
    
    def stop(self):
        """Stop the Spark context."""
        duration = (datetime.now() - self.start_time).total_seconds()
        logger.info(f"Spark Context stopped. Duration: {duration}s")


class RDD:
    """Simulate Spark RDD (Resilient Distributed Dataset)."""
    
    def __init__(self, data: List, sc: SparkContext, num_partitions: int = 4):
        self.sc = sc
        self.partitions = self._partition_data(data, num_partitions)
        self.lineage = []
        
    def _partition_data(self, data: List, num_partitions: int) -> List[List]:
        """Partition data across workers."""
        partitions = [[] for _ in range(num_partitions)]
        for i, item in enumerate(data):
            partitions[i % num_partitions].append(item)
        return partitions
    
    def map(self, func) -> 'RDD':
        """Apply a function to each element."""
        new_partitions = []
        for partition in self.partitions:
            new_partitions.append([func(item) for item in partition])
        
        new_rdd = RDD([], self.sc, len(new_partitions))
        new_rdd.partitions = new_partitions
        new_rdd.lineage = self.lineage + ['map']
        return new_rdd
    
    def filter(self, func) -> 'RDD':
        """Filter elements based on a predicate."""
        new_partitions = []
        for partition in self.partitions:
            new_partitions.append([item for item in partition if func(item)])
        
        new_rdd = RDD([], self.sc, len(new_partitions))
        new_rdd.partitions = new_partitions
        new_rdd.lineage = self.lineage + ['filter']
        return new_rdd
    
    def flatMap(self, func) -> 'RDD':
        """Apply function and flatten results."""
        new_partitions = []
        for partition in self.partitions:
            flattened = []
            for item in partition:
                result = func(item)
                if isinstance(result, (list, tuple)):
                    flattened.extend(result)
                else:
                    flattened.append(result)
            new_partitions.append(flattened)
        
        new_rdd = RDD([], self.sc, len(new_partitions))
        new_rdd.partitions = new_partitions
        new_rdd.lineage = self.lineage + ['flatMap']
        return new_rdd
    
    def reduceByKey(self, func) -> 'RDD':
        """Merge values for each key using a function."""
        # Combine all partitions
        combined = defaultdict(list)
        for partition in self.partitions:
            for key, value in partition:
                combined[key].append(value)
        
        # Reduce
        reduced = [(key, func(values)) for key, values in combined.items()]
        
        new_rdd = RDD(reduced, self.sc, len(self.partitions))
        new_rdd.lineage = self.lineage + ['reduceByKey']
        return new_rdd
    
    def groupByKey(self) -> 'RDD':
        """Group values by key."""
        combined = defaultdict(list)
        for partition in self.partitions:
            for key, value in partition:
                combined[key].append(value)
        
        result = [(key, values) for key, values in combined.items()]
        return RDD(result, self.sc, len(self.partitions))
    
    def distinct(self) -> 'RDD':
        """Return distinct elements."""
        seen = set()
        distinct_data = []
        for partition in self.partitions:
            for item in partition:
                if item not in seen:
                    seen.add(item)
                    distinct_data.append(item)
        
        return RDD(distinct_data, self.sc, len(self.partitions))
    
    def union(self, other: 'RDD') -> 'RDD':
        """Union of two RDDs."""
        combined = []
        for partition in self.partitions:
            combined.extend(partition)
        for partition in other.partitions:
            combined.extend(partition)
        
        return RDD(combined, self.sc, max(len(self.partitions), len(other.partitions)))
    
    def count(self) -> int:
        """Count elements in RDD."""
        return sum(len(partition) for partition in self.partitions)
    
    def collect(self) -> List:
        """Collect all elements to driver."""
        result = []
        for partition in self.partitions:
            result.extend(partition)
        return result
    
    def take(self, n: int) -> List:
        """Take first n elements."""
        result = []
        for partition in self.partitions:
            for item in partition:
                if len(result) < n:
                    result.append(item)
                else:
                    return result
        return result
    
    def first(self):
        """Return first element."""
        for partition in self.partitions:
            if partition:
                return partition[0]
        return None
    
    def reduce(self, func):
        """Aggregate elements using a function."""
        all_data = self.collect()
        if not all_data:
            return None
        
        result = all_data[0]
        for item in all_data[1:]:
            result = func(result, item)
        return result
    
    def foreach(self, func):
        """Apply function to each element (side effect)."""
        for partition in self.partitions:
            for item in partition:
                func(item)
    
    def cache(self) -> 'RDD':
        """Persist RDD in memory."""
        logger.info(f"Caching RDD with {self.count()} elements")
        self.is_cached = True
        return self
    
    def getNumPartitions(self) -> int:
        """Get number of partitions."""
        return len(self.partitions)


class SparkJob:
    """Spark job for processing large resume datasets."""
    
    def __init__(self, app_name: str):
        self.sc = SparkContext(app_name)
        
    def generate_resume_data(self, n_records: int = 10000) -> List[Dict]:
        """Generate sample resume data."""
        first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Chris', 'Jessica']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']
        skills_pool = ['Python', 'Java', 'SQL', 'Spark', 'AWS', 'Azure', 'Docker', 'Kubernetes', 'ML']
        locations = ['New York', 'Los Angeles', 'Chicago', 'Seattle', 'Austin', 'Boston']
        
        resumes = []
        for i in range(n_records):
            resume = {
                'id': f'RES{i:06d}',
                'name': f'{random.choice(first_names)} {random.choice(last_names)}',
                'location': random.choice(locations),
                'experience_years': random.randint(0, 20),
                'salary': random.randint(40000, 200000),
                'skills': random.sample(skills_pool, random.randint(2, 6)),
                'education': random.choice(['BS', 'MS', 'PhD']),
                'is_active': random.choice([True, True, True, False])
            }
            resumes.append(resume)
        
        return resumes
    
    def process_resumes(self, resumes: List[Dict]) -> Dict:
        """Process resumes using Spark RDD operations."""
        logger.info("Starting resume processing job...")
        
        # Create RDD
        resumes_rdd = self.sc.parallelize(resumes, num_partitions=8)
        
        # Filter active candidates
        active_rdd = resumes_rdd.filter(lambda x: x['is_active'])
        
        # Map to extract key information
        mapped_rdd = active_rdd.map(lambda x: (
            x['location'],
            {
                'name': x['name'],
                'experience': x['experience_years'],
                'salary': x['salary'],
                'skill_count': len(x['skills'])
            }
        ))
        
        # Group by location
        by_location = mapped_rdd.groupByKey()
        
        # Calculate statistics per location
        location_stats = by_location.map(lambda x: (
            x[0],
            {
                'count': len(x[1]),
                'avg_experience': sum(c['experience'] for c in x[1]) / len(x[1]),
                'avg_salary': sum(c['salary'] for c in x[1]) / len(x[1]),
                'total_skills': sum(c['skill_count'] for c in x[1])
            }
        ))
        
        # Collect results
        results = location_stats.collect()
        
        # Additional analysis: Skill frequency
        skills_rdd = active_rdd.flatMap(lambda x: [(skill, 1) for skill in x['skills']])
        skill_counts = skills_rdd.reduceByKey(lambda a, b: a + b)
        top_skills = skill_counts.collect()
        top_skills.sort(key=lambda x: x[1], reverse=True)
        
        # Experience distribution
        exp_rdd = active_rdd.map(lambda x: (x['experience_years'] // 5 * 5, 1))
        exp_distribution = exp_rdd.reduceByKey(lambda a, b: a + b).collect()
        
        logger.info("Resume processing completed")
        
        return {
            'location_stats': dict(results),
            'top_skills': top_skills[:10],
            'experience_distribution': sorted(exp_distribution),
            'total_processed': active_rdd.count(),
            'partitions_used': resumes_rdd.getNumPartitions()
        }
    
    def word_count_job(self, text_data: List[str]) -> Dict:
        """Classic Word Count example."""
        logger.info("Starting Word Count job...")
        
        # Create RDD
        lines_rdd = self.sc.parallelize(text_data, num_partitions=4)
        
        # Split lines into words
        words_rdd = lines_rdd.flatMap(lambda line: line.lower().split())
        
        # Clean words
        clean_words = words_rdd.map(lambda word: ''.join(c for c in word if c.isalnum()))
        clean_words = clean_words.filter(lambda word: len(word) > 2)
        
        # Map to (word, 1) pairs
        word_pairs = clean_words.map(lambda word: (word, 1))
        
        # Reduce by key
        word_counts = word_pairs.reduceByKey(lambda a, b: a + b)
        
        # Get top words
        top_words = word_counts.collect()
        top_words.sort(key=lambda x: x[1], reverse=True)
        
        logger.info("Word Count completed")
        
        return {
            'total_words': words_rdd.count(),
            'unique_words': word_counts.count(),
            'top_words': top_words[:20]
        }
    
    def run_demo(self) -> Dict:
        """Run the complete Spark basics demonstration."""
        # Generate data
        resumes = self.generate_resume_data(10000)
        
        # Process resumes
        resume_results = self.process_resumes(resumes)
        
        # Word count on resume skills
        skill_descriptions = [
            "Python is a versatile programming language used for data science",
            "Spark is a distributed computing framework for big data processing",
            "SQL is essential for database management and data querying",
            "Machine learning enables predictive analytics and AI applications",
            "Cloud computing provides scalable infrastructure for applications"
        ] * 100
        
        word_count_results = self.word_count_job(skill_descriptions)
        
        self.sc.stop()
        
        logger.info("Task 14 completed successfully!")
        
        return {
            'resume_processing': resume_results,
            'word_count': word_count_results,
            'spark_concepts': {
                'rdd': 'Resilient Distributed Dataset - fundamental data structure',
                'transformations': ['map', 'filter', 'flatMap', 'reduceByKey', 'groupByKey'],
                'actions': ['collect', 'count', 'reduce', 'first', 'take'],
                'lazy_evaluation': 'Transformations are lazy, executed when action is called',
                'lineage': 'RDD tracks dependencies for fault tolerance'
            }
        }


def main():
    """Main execution for Task 14."""
    job = SparkJob("ResumeProcessor")
    results = job.run_demo()
    
    print("\n=== Spark Resume Processing Results ===")
    print(f"\nTotal Resumes Processed: {results['resume_processing']['total_processed']:,}")
    print(f"Partitions Used: {results['resume_processing']['partitions_used']}")
    
    print("\n=== Location Statistics ===")
    for location, stats in list(results['resume_processing']['location_stats'].items())[:3]:
        print(f"  {location}: {stats['count']} candidates, Avg Salary: ${stats['avg_salary']:,.0f}")
    
    print("\n=== Top Skills ===")
    for skill, count in results['resume_processing']['top_skills'][:5]:
        print(f"  {skill}: {count} mentions")
    
    print("\n=== Word Count Results ===")
    print(f"Total Words: {results['word_count']['total_words']:,}")
    print(f"Unique Words: {results['word_count']['unique_words']:,}")
    
    return results


if __name__ == "__main__":
    main()
