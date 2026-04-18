#!/usr/bin/env python3
"""
Task 15: Spark DataFrames
Perform transformations and actions on dataset.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkDataFrame:
    """Simulate Spark DataFrame with transformations and actions."""
    
    def __init__(self, data: List[Dict], schema: Dict = None):
        self.data = data
        self.schema = schema or {}
        self.operations = []
        
    def select(self, *columns) -> 'SparkDataFrame':
        """Select specific columns."""
        new_data = []
        for row in self.data:
            new_row = {col: row.get(col) for col in columns}
            new_data.append(new_row)
        
        new_df = SparkDataFrame(new_data)
        new_df.operations = self.operations + [f"select({columns})"]
        return new_df
    
    def filter(self, condition) -> 'SparkDataFrame':
        """Filter rows based on condition."""
        new_data = [row for row in self.data if condition(row)]
        new_df = SparkDataFrame(new_data)
        new_df.operations = self.operations + ["filter(condition)"]
        return new_df
    
    def where(self, condition) -> 'SparkDataFrame':
        """Alias for filter."""
        return self.filter(condition)
    
    def withColumn(self, name: str, func) -> 'SparkDataFrame':
        """Add or replace a column."""
        new_data = []
        for row in self.data:
            new_row = row.copy()
            new_row[name] = func(row)
            new_data.append(new_row)
        
        new_df = SparkDataFrame(new_data)
        new_df.operations = self.operations + [f"withColumn({name})"]
        return new_df
    
    def withColumnRenamed(self, old_name: str, new_name: str) -> 'SparkDataFrame':
        """Rename a column."""
        new_data = []
        for row in self.data:
            new_row = row.copy()
            if old_name in new_row:
                new_row[new_name] = new_row.pop(old_name)
            new_data.append(new_row)
        
        new_df = SparkDataFrame(new_data)
        new_df.operations = self.operations + [f"withColumnRenamed({old_name}, {new_name})"]
        return new_df
    
    def drop(self, *columns) -> 'SparkDataFrame':
        """Drop columns."""
        new_data = []
        for row in self.data:
            new_row = {k: v for k, v in row.items() if k not in columns}
            new_data.append(new_row)
        
        new_df = SparkDataFrame(new_data)
        new_df.operations = self.operations + [f"drop({columns})"]
        return new_df
    
    def groupBy(self, *columns) -> 'GroupedData':
        """Group by columns."""
        return GroupedData(self, columns)
    
    def orderBy(self, column: str, ascending: bool = True) -> 'SparkDataFrame':
        """Sort by column."""
        new_data = sorted(self.data, key=lambda x: x.get(column), reverse=not ascending)
        new_df = SparkDataFrame(new_data)
        new_df.operations = self.operations + [f"orderBy({column}, {ascending})"]
        return new_df
    
    def sort(self, column: str, ascending: bool = True) -> 'SparkDataFrame':
        """Alias for orderBy."""
        return self.orderBy(column, ascending)
    
    def limit(self, n: int) -> 'SparkDataFrame':
        """Limit number of rows."""
        new_df = SparkDataFrame(self.data[:n])
        new_df.operations = self.operations + [f"limit({n})"]
        return new_df
    
    def distinct(self) -> 'SparkDataFrame':
        """Return distinct rows."""
        seen = []
        new_data = []
        for row in self.data:
            row_tuple = tuple(sorted(row.items()))
            if row_tuple not in seen:
                seen.append(row_tuple)
                new_data.append(row)
        
        new_df = SparkDataFrame(new_data)
        new_df.operations = self.operations + ["distinct()"]
        return new_df
    
    def join(self, other: 'SparkDataFrame', on: str, how: str = 'inner') -> 'SparkDataFrame':
        """Join with another DataFrame."""
        new_data = []
        
        for left_row in self.data:
            left_key = left_row.get(on)
            matched = False
            
            for right_row in other.data:
                right_key = right_row.get(on)
                
                if left_key == right_key:
                    new_row = {**left_row, **right_row}
                    new_data.append(new_row)
                    matched = True
            
            if not matched and how in ('left', 'outer'):
                new_row = {**left_row}
                for col in other.data[0].keys() if other.data else []:
                    if col not in new_row:
                        new_row[col] = None
                new_data.append(new_row)
        
        if how in ('right', 'outer'):
            left_keys = set(r.get(on) for r in self.data)
            for right_row in other.data:
                if right_row.get(on) not in left_keys:
                    new_row = {**right_row}
                    for col in self.data[0].keys() if self.data else []:
                        if col not in new_row:
                            new_row[col] = None
                    new_data.append(new_row)
        
        new_df = SparkDataFrame(new_data)
        new_df.operations = self.operations + [f"join({on}, {how})"]
        return new_df
    
    def union(self, other: 'SparkDataFrame') -> 'SparkDataFrame':
        """Union with another DataFrame."""
        new_df = SparkDataFrame(self.data + other.data)
        new_df.operations = self.operations + ["union()"]
        return new_df
    
    # Actions
    def show(self, n: int = 20):
        """Display first n rows."""
        print(f"\nShowing top {min(n, len(self.data))} rows:")
        if self.data:
            columns = list(self.data[0].keys())
            print(" | ".join(columns))
            print("-" * 80)
            for row in self.data[:n]:
                print(" | ".join(str(row.get(c, 'NULL')) for c in columns))
    
    def count(self) -> int:
        """Count rows."""
        return len(self.data)
    
    def collect(self) -> List[Dict]:
        """Collect all rows."""
        return self.data
    
    def first(self) -> Dict:
        """Get first row."""
        return self.data[0] if self.data else None
    
    def take(self, n: int) -> List[Dict]:
        """Take first n rows."""
        return self.data[:n]
    
    def head(self, n: int = 1) -> List[Dict]:
        """Get first n rows."""
        return self.take(n)
    
    def toPandas(self):
        """Convert to pandas DataFrame."""
        try:
            import pandas as pd
            return pd.DataFrame(self.data)
        except ImportError:
            logger.warning("pandas not available")
            return None
    
    def describe(self) -> Dict:
        """Generate descriptive statistics."""
        numeric_cols = []
        for row in self.data:
            for key, value in row.items():
                if isinstance(value, (int, float)):
                    numeric_cols.append(key)
            break
        
        stats = {}
        for col in numeric_cols:
            values = [r.get(col) for r in self.data if r.get(col) is not None]
            if values:
                stats[col] = {
                    'count': len(values),
                    'mean': sum(values) / len(values),
                    'min': min(values),
                    'max': max(values)
                }
        
        return stats
    
    def printSchema(self):
        """Print schema."""
        if self.data:
            print("root")
            for key, value in self.data[0].items():
                dtype = type(value).__name__
                print(f" |-- {key}: {dtype} (nullable = true)")


class GroupedData:
    """Grouped DataFrame for aggregations."""
    
    def __init__(self, df: SparkDataFrame, columns: tuple):
        self.df = df
        self.columns = columns
        
    def agg(self, *aggs) -> SparkDataFrame:
        """Perform aggregations."""
        # Group data
        groups = {}
        for row in self.df.data:
            key = tuple(row.get(c) for c in self.columns)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        
        # Apply aggregations
        result = []
        for key, rows in groups.items():
            new_row = dict(zip(self.columns, key))
            
            for agg_func in aggs:
                col_name, func = agg_func
                values = [r.get(col_name) for r in rows if r.get(col_name) is not None]
                
                if func == 'count':
                    new_row[f'{col_name}_count'] = len(values)
                elif func == 'sum':
                    new_row[f'{col_name}_sum'] = sum(values)
                elif func == 'avg':
                    new_row[f'{col_name}_avg'] = sum(values) / len(values) if values else 0
                elif func == 'min':
                    new_row[f'{col_name}_min'] = min(values) if values else None
                elif func == 'max':
                    new_row[f'{col_name}_max'] = max(values) if values else None
            
            result.append(new_row)
        
        return SparkDataFrame(result)
    
    def count(self) -> SparkDataFrame:
        """Count rows per group."""
        return self.agg(('any', 'count'))
    
    def sum(self, column: str) -> SparkDataFrame:
        """Sum column per group."""
        return self.agg((column, 'sum'))
    
    def avg(self, column: str) -> SparkDataFrame:
        """Average column per group."""
        return self.agg((column, 'avg'))
    
    def min(self, column: str) -> SparkDataFrame:
        """Min column per group."""
        return self.agg((column, 'min'))
    
    def max(self, column: str) -> SparkDataFrame:
        """Max column per group."""
        return self.agg((column, 'max'))


class DataFrameDemo:
    """Demonstrate Spark DataFrame operations."""
    
    def __init__(self):
        self.df = None
        
    def generate_data(self, n: int = 1000) -> List[Dict]:
        """Generate sample resume data."""
        first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones']
        locations = ['New York', 'Los Angeles', 'Chicago', 'Seattle', 'Austin']
        skills_list = ['Python', 'Java', 'SQL', 'Spark', 'AWS', 'Azure', 'Docker']
        
        data = []
        for i in range(n):
            data.append({
                'id': i + 1,
                'name': f'{random.choice(first_names)} {random.choice(last_names)}',
                'location': random.choice(locations),
                'department': random.choice(['Engineering', 'Data Science', 'DevOps']),
                'experience_years': random.randint(0, 20),
                'salary': random.randint(50000, 200000),
                'skills': random.sample(skills_list, random.randint(2, 5)),
                'is_active': random.choice([True, False])
            })
        
        return data
    
    def run_transformations_demo(self) -> Dict:
        """Run DataFrame transformations demonstration."""
        # Create DataFrame
        data = self.generate_data(1000)
        df = SparkDataFrame(data)
        
        logger.info("Starting DataFrame transformations demo...")
        
        # 1. Select columns
        selected = df.select('name', 'location', 'salary')
        
        # 2. Filter rows
        filtered = df.filter(lambda x: x['is_active'] == True)
        
        # 3. Add computed column
        with_category = df.withColumn('salary_category', 
            lambda x: 'High' if x['salary'] > 150000 else 'Medium' if x['salary'] > 80000 else 'Low')
        
        # 4. Group by and aggregate
        dept_stats = df.groupBy('department').agg(
            ('salary', 'avg'),
            ('salary', 'max'),
            ('id', 'count')
        )
        
        # 5. Sort
        sorted_df = df.orderBy('salary', ascending=False)
        
        # 6. Join with another DataFrame
        dept_info = SparkDataFrame([
            {'department': 'Engineering', 'budget': 5000000, 'head_count': 50},
            {'department': 'Data Science', 'budget': 3000000, 'head_count': 30},
            {'department': 'DevOps', 'budget': 2000000, 'head_count': 20}
        ])
        
        joined = df.join(dept_info, on='department', how='left')
        
        logger.info("DataFrame transformations demo completed")
        
        return {
            'original_count': df.count(),
            'active_count': filtered.count(),
            'department_stats': dept_stats.collect(),
            'top_earners': sorted_df.take(5),
            'operations': sorted_df.operations
        }
    
    def run_actions_demo(self) -> Dict:
        """Run DataFrame actions demonstration."""
        data = self.generate_data(100)
        df = SparkDataFrame(data)
        
        logger.info("Starting DataFrame actions demo...")
        
        # Various actions
        count = df.count()
        first_row = df.first()
        top_10 = df.take(10)
        all_data = df.collect()
        stats = df.describe()
        
        logger.info("DataFrame actions demo completed")
        
        return {
            'count': count,
            'first_row': first_row,
            'sample_rows': top_10[:3],
            'statistics': stats
        }
    
    def run_demo(self) -> Dict:
        """Run complete DataFrame demonstration."""
        transformations = self.run_transformations_demo()
        actions = self.run_actions_demo()
        
        logger.info("Task 15 completed successfully!")
        
        return {
            'transformations': transformations,
            'actions': actions,
            'dataframe_concepts': {
                'transformations': [
                    'select() - Select columns',
                    'filter()/where() - Filter rows',
                    'withColumn() - Add/replace column',
                    'groupBy() - Group for aggregation',
                    'join() - Combine DataFrames',
                    'orderBy()/sort() - Sort rows',
                    'distinct() - Remove duplicates'
                ],
                'actions': [
                    'show() - Display rows',
                    'count() - Count rows',
                    'collect() - Get all rows',
                    'first() - Get first row',
                    'take() - Get n rows',
                    'describe() - Statistics'
                ],
                'benefits': [
                    'Optimized execution (Catalyst optimizer)',
                    'Schema enforcement',
                    'SQL-like operations',
                    'Better performance than RDDs'
                ]
            }
        }


def main():
    """Main execution for Task 15."""
    demo = DataFrameDemo()
    results = demo.run_demo()
    
    print("\n=== Spark DataFrame Transformations ===")
    print(f"Original Count: {results['transformations']['original_count']:,}")
    print(f"Active Candidates: {results['transformations']['active_count']:,}")
    
    print("\n=== Department Statistics ===")
    for dept in results['transformations']['department_stats']:
        print(f"  {dept['department']}: Avg Salary ${dept.get('salary_avg', 0):,.0f}, "
              f"Count: {dept.get('id_count', 0)}")
    
    print("\n=== Top Earners ===")
    for person in results['transformations']['top_earners'][:3]:
        print(f"  {person['name']}: ${person['salary']:,}")
    
    return results


if __name__ == "__main__":
    main()
