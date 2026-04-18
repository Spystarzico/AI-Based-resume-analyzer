#!/usr/bin/env python3
"""
Task 16: Spark SQL
Query large dataset using Spark SQL.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any
import random
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkSession:
    """Simulate SparkSession for SQL operations."""
    
    def __init__(self, app_name: str):
        self.app_name = app_name
        self.tables = {}
        logger.info(f"SparkSession created: {app_name}")
    
    def createDataFrame(self, data: List[Dict], schema=None):
        """Create a DataFrame from data."""
        return SparkSQLDataFrame(data, schema, self)
    
    def sql(self, query: str) -> 'SparkSQLDataFrame':
        """Execute SQL query."""
        logger.info(f"Executing SQL: {query}")
        return self._execute_sql(query)
    
    def _execute_sql(self, query: str) -> 'SparkSQLDataFrame':
        """Parse and execute SQL query."""
        query = query.strip()
        
        # Simple SQL parser
        if query.upper().startswith('SELECT'):
            return self._execute_select(query)
        elif query.upper().startswith('CREATE'):
            return self._execute_create(query)
        elif query.upper().startswith('INSERT'):
            return self._execute_insert(query)
        else:
            raise ValueError(f"Unsupported query: {query}")
    
    def _execute_select(self, query: str) -> 'SparkSQLDataFrame':
        """Execute SELECT query."""
        # Extract table name
        from_match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
        if not from_match:
            raise ValueError("No FROM clause found")
        
        table_name = from_match.group(1)
        if table_name not in self.tables:
            raise ValueError(f"Table not found: {table_name}")
        
        data = self.tables[table_name].data
        
        # Handle WHERE clause
        where_match = re.search(r'WHERE\s+(.+?)(?:GROUP|ORDER|LIMIT|$)', query, re.IGNORECASE)
        if where_match:
            condition = where_match.group(1).strip()
            data = self._apply_where(data, condition)
        
        # Handle GROUP BY
        group_match = re.search(r'GROUP\s+BY\s+(\w+)', query, re.IGNORECASE)
        if group_match:
            group_col = group_match.group(1)
            data = self._apply_groupby(data, group_col, query)
        
        # Handle ORDER BY
        order_match = re.search(r'ORDER\s+BY\s+(\w+)(?:\s+(ASC|DESC))?', query, re.IGNORECASE)
        if order_match:
            order_col = order_match.group(1)
            order_dir = order_match.group(2) or 'ASC'
            data = sorted(data, key=lambda x: x.get(order_col), reverse=(order_dir.upper() == 'DESC'))
        
        # Handle LIMIT
        limit_match = re.search(r'LIMIT\s+(\d+)', query, re.IGNORECASE)
        if limit_match:
            limit = int(limit_match.group(1))
            data = data[:limit]
        
        # Handle column selection
        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', query, re.IGNORECASE)
        if select_match:
            columns = select_match.group(1).strip()
            if columns != '*':
                data = self._apply_select_columns(data, columns)
        
        return SparkSQLDataFrame(data, None, self)
    
    def _apply_where(self, data: List[Dict], condition: str) -> List[Dict]:
        """Apply WHERE condition."""
        result = []
        for row in data:
            if self._evaluate_condition(row, condition):
                result.append(row)
        return result
    
    def _evaluate_condition(self, row: Dict, condition: str) -> bool:
        """Evaluate a condition against a row."""
        # Simple condition evaluation
        condition = condition.strip()
        
        # Handle AND/OR
        if ' AND ' in condition.upper():
            parts = re.split(r'\s+AND\s+', condition, flags=re.IGNORECASE)
            return all(self._evaluate_condition(row, p) for p in parts)
        
        if ' OR ' in condition.upper():
            parts = re.split(r'\s+OR\s+', condition, flags=re.IGNORECASE)
            return any(self._evaluate_condition(row, p) for p in parts)
        
        # Handle comparisons
        match = re.match(r'(\w+)\s*([=<>!]+)\s*(.+)', condition)
        if match:
            col, op, val = match.groups()
            col_val = row.get(col)
            
            # Try to convert value
            try:
                val = int(val)
            except:
                try:
                    val = float(val)
                except:
                    val = val.strip("'\"")
            
            if op == '=':
                return col_val == val
            elif op == '!=':
                return col_val != val
            elif op == '>':
                return col_val is not None and col_val > val
            elif op == '<':
                return col_val is not None and col_val < val
            elif op == '>=':
                return col_val is not None and col_val >= val
            elif op == '<=':
                return col_val is not None and col_val <= val
        
        return True
    
    def _apply_groupby(self, data: List[Dict], group_col: str, query: str) -> List[Dict]:
        """Apply GROUP BY aggregation."""
        groups = {}
        for row in data:
            key = row.get(group_col)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        
        result = []
        for key, rows in groups.items():
            new_row = {group_col: key}
            
            # Extract aggregation functions
            agg_matches = re.findall(r'(COUNT|SUM|AVG|MIN|MAX)\((\*|\w+)\)', query, re.IGNORECASE)
            for func, col in agg_matches:
                func = func.upper()
                if func == 'COUNT':
                    new_row[f'count({col})'] = len(rows)
                elif func == 'SUM' and col != '*':
                    values = [r.get(col, 0) for r in rows if r.get(col) is not None]
                    new_row[f'sum({col})'] = sum(values)
                elif func == 'AVG' and col != '*':
                    values = [r.get(col, 0) for r in rows if r.get(col) is not None]
                    new_row[f'avg({col})'] = sum(values) / len(values) if values else 0
                elif func == 'MIN' and col != '*':
                    values = [r.get(col) for r in rows if r.get(col) is not None]
                    new_row[f'min({col})'] = min(values) if values else None
                elif func == 'MAX' and col != '*':
                    values = [r.get(col) for r in rows if r.get(col) is not None]
                    new_row[f'max({col})'] = max(values) if values else None
            
            result.append(new_row)
        
        return result
    
    def _apply_select_columns(self, data: List[Dict], columns: str) -> List[Dict]:
        """Apply column selection."""
        col_list = [c.strip() for c in columns.split(',')]
        
        result = []
        for row in data:
            new_row = {}
            for col in col_list:
                # Handle aliases
                if ' AS ' in col.upper():
                    parts = re.split(r'\s+AS\s+', col, flags=re.IGNORECASE)
                    source = parts[0].strip()
                    alias = parts[1].strip()
                    
                    # Handle functions
                    func_match = re.match(r'(\w+)\((\w+)\)', source)
                    if func_match:
                        new_row[alias] = row.get(source)
                    else:
                        new_row[alias] = row.get(source)
                else:
                    new_row[col] = row.get(col)
            result.append(new_row)
        
        return result
    
    def _execute_create(self, query: str) -> 'SparkSQLDataFrame':
        """Execute CREATE TABLE query."""
        match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?TEMP\s+VIEW\s+(\w+)', query, re.IGNORECASE)
        if match:
            view_name = match.group(1)
            # Extract the SELECT part
            select_match = re.search(r'AS\s+(SELECT.+)', query, re.IGNORECASE)
            if select_match:
                select_query = select_match.group(1)
                result = self._execute_select(select_query)
                self.tables[view_name] = result
                logger.info(f"Created temporary view: {view_name}")
                return result
        
        return SparkSQLDataFrame([], None, self)
    
    def _execute_insert(self, query: str) -> 'SparkSQLDataFrame':
        """Execute INSERT query."""
        logger.info("INSERT operation simulated")
        return SparkSQLDataFrame([], None, self)


class SparkSQLDataFrame:
    """DataFrame for Spark SQL results."""
    
    def __init__(self, data: List[Dict], schema, session: SparkSession):
        self.data = data
        self.schema = schema
        self.session = session
    
    def show(self, n: int = 20):
        """Display results."""
        print(f"\nShowing {min(n, len(self.data))} rows:")
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
    
    def createOrReplaceTempView(self, name: str):
        """Create temporary view."""
        self.session.tables[name] = self
        logger.info(f"Created temp view: {name}")


class SparkSQLDemo:
    """Demonstrate Spark SQL capabilities."""
    
    def __init__(self):
        self.spark = SparkSession("SparkSQLDemo")
    
    def generate_resume_data(self, n: int = 1000) -> List[Dict]:
        """Generate sample resume data."""
        first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones']
        locations = ['New York', 'Los Angeles', 'Chicago', 'Seattle', 'Austin']
        departments = ['Engineering', 'Data Science', 'DevOps', 'Product', 'Sales']
        
        data = []
        for i in range(n):
            data.append({
                'id': i + 1,
                'name': f'{random.choice(first_names)} {random.choice(last_names)}',
                'location': random.choice(locations),
                'department': random.choice(departments),
                'experience_years': random.randint(0, 20),
                'salary': random.randint(50000, 200000),
                'bonus': random.randint(5000, 50000),
                'is_active': random.choice([True, False])
            })
        
        return data
    
    def run_sql_queries(self) -> Dict:
        """Run various SQL queries."""
        # Create DataFrame and register as table
        data = self.generate_resume_data(1000)
        df = self.spark.createDataFrame(data)
        df.createOrReplaceTempView('candidates')
        
        results = {}
        
        # Query 1: Basic SELECT
        results['basic_select'] = self.spark.sql('''
            SELECT name, location, salary 
            FROM candidates 
            LIMIT 5
        ''').collect()
        
        # Query 2: WHERE clause
        results['filtered'] = self.spark.sql('''
            SELECT name, department, salary
            FROM candidates
            WHERE salary > 150000 AND is_active = True
            ORDER BY salary DESC
            LIMIT 10
        ''').collect()
        
        # Query 3: GROUP BY with aggregation
        results['grouped'] = self.spark.sql('''
            SELECT 
                department,
                COUNT(*) as count,
                AVG(salary) as avg_salary,
                MAX(salary) as max_salary,
                MIN(salary) as min_salary
            FROM candidates
            WHERE is_active = True
            GROUP BY department
            ORDER BY avg_salary DESC
        ''').collect()
        
        # Query 4: Complex query with multiple conditions
        results['complex'] = self.spark.sql('''
            SELECT 
                location,
                department,
                COUNT(*) as candidate_count,
                AVG(salary + bonus) as total_compensation
            FROM candidates
            WHERE experience_years >= 5
            GROUP BY location, department
            HAVING candidate_count >= 10
            ORDER BY total_compensation DESC
            LIMIT 10
        ''').collect()
        
        # Query 5: Create temporary view
        high_earners = self.spark.sql('''
            CREATE OR REPLACE TEMP VIEW high_earners AS
            SELECT * FROM candidates WHERE salary > 150000
        ''')
        
        results['view_query'] = self.spark.sql('''
            SELECT department, COUNT(*) as high_earner_count
            FROM high_earners
            GROUP BY department
            ORDER BY high_earner_count DESC
        ''').collect()
        
        return results
    
    def run_demo(self) -> Dict:
        """Run complete Spark SQL demonstration."""
        query_results = self.run_sql_queries()
        
        logger.info("Task 16 completed successfully!")
        
        return {
            'query_results': query_results,
            'spark_sql_features': {
                'sql_interface': 'Run SQL queries on structured data',
                'hive_integration': 'Query Hive tables directly',
                'performance': 'Catalyst optimizer for query optimization',
                'udfs': 'Support for user-defined functions',
                'views': 'Create temporary and persistent views',
                'joins': 'Support for all SQL join types'
            }
        }


def main():
    """Main execution for Task 16."""
    demo = SparkSQLDemo()
    results = demo.run_demo()
    
    print("\n=== Spark SQL Query Results ===")
    
    print("\n1. Basic SELECT:")
    for row in results['query_results']['basic_select'][:3]:
        print(f"  {row['name']}: ${row['salary']:,}")
    
    print("\n2. Department Statistics:")
    for row in results['query_results']['grouped']:
        print(f"  {row['department']}: {row['count({*})']} employees, "
              f"Avg Salary: ${row.get('avg(salary)', 0):,.0f}")
    
    print("\n3. High Earners by Department:")
    for row in results['query_results']['view_query']:
        print(f"  {row['department']}: {row['high_earner_count']} high earners")
    
    return results


if __name__ == "__main__":
    main()
