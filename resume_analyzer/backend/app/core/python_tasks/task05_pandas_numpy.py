#!/usr/bin/env python3
"""
Task 5: Pandas + NumPy
Perform large-scale data analysis (1M+ rows). Optimize memory usage and compare performance.
"""

import numpy as np
import pandas as pd
import time
import logging
from typing import Dict, List, Tuple
import gc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LargeScaleDataAnalyzer:
    """Analyze large-scale resume datasets with memory optimization."""
    
    def __init__(self, n_rows: int = 1000000):
        self.n_rows = n_rows
        self.data = None
        self.optimized_data = None
        self.performance_results = {}
        
    def generate_large_dataset(self) -> pd.DataFrame:
        """Generate a large synthetic resume dataset."""
        logger.info(f"Generating dataset with {self.n_rows:,} rows...")
        
        np.random.seed(42)
        
        # Generate data in chunks to avoid memory issues
        chunk_size = 100000
        chunks = []
        
        skills_pool = ['Python', 'Java', 'SQL', 'Spark', 'AWS', 'Azure', 'Docker', 'Kubernetes',
                      'Machine Learning', 'Deep Learning', 'TensorFlow', 'PyTorch', 'Pandas',
                      'NumPy', 'Scikit-learn', 'Tableau', 'PowerBI', 'Excel', 'R', 'Scala']
        
        education_levels = ['High School', 'Bachelor', 'Master', 'PhD', 'Certificate']
        job_titles = ['Data Engineer', 'Data Scientist', 'ML Engineer', 'Software Engineer',
                     'Data Analyst', 'BI Developer', 'Cloud Architect', 'DevOps Engineer']
        
        for i in range(0, self.n_rows, chunk_size):
            current_chunk_size = min(chunk_size, self.n_rows - i)
            
            chunk = pd.DataFrame({
                'candidate_id': [f'C{str(j).zfill(8)}' for j in range(i, i + current_chunk_size)],
                'name': [f'Candidate_{j}' for j in range(i, i + current_chunk_size)],
                'age': np.random.randint(22, 60, current_chunk_size),
                'experience_years': np.random.randint(0, 35, current_chunk_size),
                'salary_expectation': np.random.randint(30000, 200000, current_chunk_size),
                'skill_count': np.random.randint(1, 15, current_chunk_size),
                'primary_skill': np.random.choice(skills_pool, current_chunk_size),
                'education': np.random.choice(education_levels, current_chunk_size),
                'desired_role': np.random.choice(job_titles, current_chunk_size),
                'willing_to_relocate': np.random.choice([True, False], current_chunk_size),
                'remote_preference': np.random.choice([0, 1, 2], current_chunk_size),  # 0=office, 1=hybrid, 2=remote
                'match_score': np.random.uniform(0, 100, current_chunk_size).round(2),
                'application_date': pd.date_range(start='2023-01-01', periods=current_chunk_size, freq='min'),
                'is_active': np.random.choice([True, False], current_chunk_size, p=[0.8, 0.2]),
            })
            
            chunks.append(chunk)
            logger.info(f"Generated chunk {i//chunk_size + 1}/{(self.n_rows-1)//chunk_size + 1}")
        
        self.data = pd.concat(chunks, ignore_index=True)
        logger.info(f"Dataset generated: {self.data.shape}")
        
        return self.data
    
    def get_memory_usage(self, df: pd.DataFrame) -> Dict:
        """Get detailed memory usage statistics."""
        memory_mb = df.memory_usage(deep=True).sum() / 1024**2
        
        per_column = {}
        for col in df.columns:
            col_memory = df[col].memory_usage(deep=True) / 1024**2
            per_column[col] = {
                'memory_mb': round(col_memory, 4),
                'dtype': str(df[col].dtype),
                'percentage': round(col_memory / memory_mb * 100, 2)
            }
        
        return {
            'total_mb': round(memory_mb, 4),
            'total_gb': round(memory_mb / 1024, 4),
            'per_column': per_column,
            'row_count': len(df)
        }
    
    def optimize_memory(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize DataFrame memory usage."""
        logger.info("Optimizing memory usage...")
        
        start_mem = df.memory_usage(deep=True).sum() / 1024**2
        
        optimized = df.copy()
        
        # Optimize integers
        for col in optimized.select_dtypes(include=['int']).columns:
            col_min = optimized[col].min()
            col_max = optimized[col].max()
            
            if col_min >= 0:
                if col_max < 255:
                    optimized[col] = optimized[col].astype(np.uint8)
                elif col_max < 65535:
                    optimized[col] = optimized[col].astype(np.uint16)
                elif col_max < 4294967295:
                    optimized[col] = optimized[col].astype(np.uint32)
            else:
                if col_min > np.iinfo(np.int8).min and col_max < np.iinfo(np.int8).max:
                    optimized[col] = optimized[col].astype(np.int8)
                elif col_min > np.iinfo(np.int16).min and col_max < np.iinfo(np.int16).max:
                    optimized[col] = optimized[col].astype(np.int16)
                elif col_min > np.iinfo(np.int32).min and col_max < np.iinfo(np.int32).max:
                    optimized[col] = optimized[col].astype(np.int32)
        
        # Optimize floats
        for col in optimized.select_dtypes(include=['float']).columns:
            optimized[col] = optimized[col].astype(np.float32)
        
        # Optimize objects (strings) to category
        for col in optimized.select_dtypes(include=['object']).columns:
            num_unique = optimized[col].nunique()
            num_total = len(optimized[col])
            
            if num_unique / num_total < 0.5:  # If less than 50% unique values
                optimized[col] = optimized[col].astype('category')
        
        # Optimize booleans
        for col in optimized.select_dtypes(include=['bool']).columns:
            optimized[col] = optimized[col].astype(np.bool_)
        
        end_mem = optimized.memory_usage(deep=True).sum() / 1024**2
        
        logger.info(f"Memory optimized: {start_mem:.2f} MB -> {end_mem:.2f} MB "
                   f"({(start_mem - end_mem) / start_mem * 100:.1f}% reduction)")
        
        self.optimized_data = optimized
        return optimized
    
    def benchmark_operation(self, operation_name: str, 
                           regular_func, optimized_func) -> Dict:
        """Benchmark operation on regular vs optimized data."""
        logger.info(f"Benchmarking: {operation_name}")
        
        # Benchmark regular data
        gc.collect()
        start = time.time()
        regular_result = regular_func()
        regular_time = time.time() - start
        
        # Benchmark optimized data
        gc.collect()
        start = time.time()
        optimized_result = optimized_func()
        optimized_time = time.time() - start
        
        result = {
            'operation': operation_name,
            'regular_time': round(regular_time, 4),
            'optimized_time': round(optimized_time, 4),
            'speedup': round(regular_time / optimized_time, 2) if optimized_time > 0 else float('inf'),
            'time_saved_pct': round((regular_time - optimized_time) / regular_time * 100, 2)
        }
        
        self.performance_results[operation_name] = result
        return result
    
    def run_performance_comparison(self) -> Dict:
        """Run comprehensive performance comparison."""
        logger.info("Running performance comparison...")
        
        if self.data is None:
            self.generate_large_dataset()
        
        if self.optimized_data is None:
            self.optimize_memory(self.data)
        
        comparisons = []
        
        # 1. Filtering
        comparisons.append(self.benchmark_operation(
            'Filter by experience',
            lambda: self.data[self.data['experience_years'] > 5],
            lambda: self.optimized_data[self.optimized_data['experience_years'] > 5]
        ))
        
        # 2. Groupby aggregation
        comparisons.append(self.benchmark_operation(
            'Groupby education level',
            lambda: self.data.groupby('education')['match_score'].mean(),
            lambda: self.optimized_data.groupby('education')['match_score'].mean()
        ))
        
        # 3. Sorting
        comparisons.append(self.benchmark_operation(
            'Sort by match_score',
            lambda: self.data.sort_values('match_score', ascending=False),
            lambda: self.optimized_data.sort_values('match_score', ascending=False)
        ))
        
        # 4. Multiple column operations
        comparisons.append(self.benchmark_operation(
            'Complex query',
            lambda: self.data[(self.data['experience_years'] > 3) & 
                            (self.data['match_score'] > 70) &
                            (self.data['is_active'] == True)],
            lambda: self.optimized_data[(self.optimized_data['experience_years'] > 3) & 
                                       (self.optimized_data['match_score'] > 70) &
                                       (self.optimized_data['is_active'] == True)]
        ))
        
        # 5. Value counts
        comparisons.append(self.benchmark_operation(
            'Value counts',
            lambda: self.data['primary_skill'].value_counts(),
            lambda: self.optimized_data['primary_skill'].value_counts()
        ))
        
        return {
            'memory_comparison': {
                'regular': self.get_memory_usage(self.data),
                'optimized': self.get_memory_usage(self.optimized_data)
            },
            'performance_comparison': comparisons
        }
    
    def analyze_dataset(self) -> Dict:
        """Perform comprehensive analysis on the dataset."""
        if self.data is None:
            self.generate_large_dataset()
        
        logger.info("Performing comprehensive analysis...")
        
        analysis = {
            'basic_stats': {
                'total_candidates': len(self.data),
                'avg_experience': round(self.data['experience_years'].mean(), 2),
                'avg_match_score': round(self.data['match_score'].mean(), 2),
                'avg_salary_expectation': round(self.data['salary_expectation'].mean(), 2),
            },
            'skill_distribution': self.data['primary_skill'].value_counts().head(10).to_dict(),
            'education_distribution': self.data['education'].value_counts().to_dict(),
            'experience_distribution': {
                '0-2 years': len(self.data[self.data['experience_years'] <= 2]),
                '3-5 years': len(self.data[(self.data['experience_years'] > 2) & 
                                          (self.data['experience_years'] <= 5)]),
                '6-10 years': len(self.data[(self.data['experience_years'] > 5) & 
                                           (self.data['experience_years'] <= 10)]),
                '10+ years': len(self.data[self.data['experience_years'] > 10])
            },
            'top_performers': self.data.nlargest(5, 'match_score')[['candidate_id', 'name', 
                                                                     'match_score', 
                                                                     'primary_skill']].to_dict('records'),
            'correlation_matrix': self.data[['age', 'experience_years', 'salary_expectation', 
                                            'skill_count', 'match_score']].corr().round(3).to_dict()
        }
        
        return analysis


def main():
    """Main execution for Task 5."""
    # Initialize analyzer with 1M rows
    analyzer = LargeScaleDataAnalyzer(n_rows=1000000)
    
    # Generate dataset
    df = analyzer.generate_large_dataset()
    
    # Get initial memory usage
    initial_memory = analyzer.get_memory_usage(df)
    logger.info(f"Initial memory usage: {initial_memory['total_mb']:.2f} MB")
    
    # Optimize memory
    optimized_df = analyzer.optimize_memory(df)
    optimized_memory = analyzer.get_memory_usage(optimized_df)
    logger.info(f"Optimized memory usage: {optimized_memory['total_mb']:.2f} MB")
    
    # Run performance comparison
    comparison = analyzer.run_performance_comparison()
    
    # Perform analysis
    analysis = analyzer.analyze_dataset()
    
    logger.info("Task 5 completed successfully!")
    
    print("\n=== Memory Usage Comparison ===")
    print(f"Regular: {initial_memory['total_mb']:.2f} MB ({initial_memory['total_gb']:.4f} GB)")
    print(f"Optimized: {optimized_memory['total_mb']:.2f} MB ({optimized_memory['total_gb']:.4f} GB)")
    print(f"Reduction: {(initial_memory['total_mb'] - optimized_memory['total_mb']) / initial_memory['total_mb'] * 100:.1f}%")
    
    print("\n=== Performance Comparison ===")
    for result in comparison['performance_comparison']:
        print(f"{result['operation']}: {result['regular_time']}s -> {result['optimized_time']}s "
              f"({result['speedup']}x speedup)")
    
    print("\n=== Dataset Analysis ===")
    print(f"Total Candidates: {analysis['basic_stats']['total_candidates']:,}")
    print(f"Average Experience: {analysis['basic_stats']['avg_experience']} years")
    print(f"Average Match Score: {analysis['basic_stats']['avg_match_score']}")
    
    return {
        'memory_comparison': comparison['memory_comparison'],
        'performance_comparison': comparison['performance_comparison'],
        'analysis': analysis
    }


if __name__ == "__main__":
    main()
