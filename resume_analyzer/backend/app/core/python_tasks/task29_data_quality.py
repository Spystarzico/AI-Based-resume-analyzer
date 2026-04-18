#!/usr/bin/env python3
"""
Task 29: Data Quality
Build validation checks and anomaly detection system.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
import random
import statistics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataQualityRule:
    """Base class for data quality rules."""
    
    def __init__(self, name: str, column: str, severity: str = 'error'):
        self.name = name
        self.column = column
        self.severity = severity  # 'error', 'warning', 'info'
        self.violations = []
    
    def validate(self, row: Dict) -> bool:
        """Validate a single row."""
        raise NotImplementedError
    
    def get_result(self) -> Dict:
        """Get validation result."""
        return {
            'rule_name': self.name,
            'column': self.column,
            'severity': self.severity,
            'violations': len(self.violations),
            'sample_violations': self.violations[:5]
        }


class CompletenessRule(DataQualityRule):
    """Check for null/empty values."""
    
    def validate(self, row: Dict) -> bool:
        value = row.get(self.column)
        if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
            self.violations.append({'row': row, 'issue': 'null or empty'})
            return False
        return True


class UniquenessRule(DataQualityRule):
    """Check for duplicate values."""
    
    def __init__(self, name: str, column: str, severity: str = 'error'):
        super().__init__(name, column, severity)
        self.seen_values = set()
    
    def validate(self, row: Dict) -> bool:
        value = row.get(self.column)
        if value in self.seen_values:
            self.violations.append({'row': row, 'issue': f'duplicate value: {value}'})
            return False
        self.seen_values.add(value)
        return True


class RangeRule(DataQualityRule):
    """Check if value is within range."""
    
    def __init__(self, name: str, column: str, min_val: float, max_val: float, severity: str = 'error'):
        super().__init__(name, column, severity)
        self.min_val = min_val
        self.max_val = max_val
    
    def validate(self, row: Dict) -> bool:
        value = row.get(self.column)
        if value is None:
            return True
        
        try:
            num_value = float(value)
            if num_value < self.min_val or num_value > self.max_val:
                self.violations.append({
                    'row': row,
                    'issue': f'value {num_value} outside range [{self.min_val}, {self.max_val}]'
                })
                return False
        except (ValueError, TypeError):
            self.violations.append({'row': row, 'issue': f'non-numeric value: {value}'})
            return False
        
        return True


class PatternRule(DataQualityRule):
    """Check if value matches pattern."""
    
    def __init__(self, name: str, column: str, pattern: str, severity: str = 'error'):
        super().__init__(name, column, severity)
        self.pattern = pattern
        import re
        self.regex = re.compile(pattern)
    
    def validate(self, row: Dict) -> bool:
        value = row.get(self.column)
        if value is None:
            return True
        
        if not self.regex.match(str(value)):
            self.violations.append({'row': row, 'issue': f'does not match pattern {self.pattern}'})
            return False
        
        return True


class ReferentialIntegrityRule(DataQualityRule):
    """Check referential integrity."""
    
    def __init__(self, name: str, column: str, reference_values: set, severity: str = 'error'):
        super().__init__(name, column, severity)
        self.reference_values = reference_values
    
    def validate(self, row: Dict) -> bool:
        value = row.get(self.column)
        if value is None:
            return True
        
        if value not in self.reference_values:
            self.violations.append({'row': row, 'issue': f'value {value} not in reference set'})
            return False
        
        return True


class AnomalyDetector:
    """Detect anomalies in data."""
    
    def __init__(self):
        self.baseline_stats = {}
        self.anomalies = []
    
    def fit(self, data: List[Dict], column: str):
        """Calculate baseline statistics."""
        values = [float(r[column]) for r in data if r.get(column) is not None]
        
        self.baseline_stats[column] = {
            'mean': statistics.mean(values),
            'std': statistics.stdev(values) if len(values) > 1 else 0,
            'median': statistics.median(values),
            'min': min(values),
            'max': max(values),
            'q1': statistics.quantiles(values, n=4)[0] if len(values) > 1 else values[0],
            'q3': statistics.quantiles(values, n=4)[2] if len(values) > 1 else values[0]
        }
    
    def detect_zscore(self, row: Dict, column: str, threshold: float = 3.0) -> bool:
        """Detect anomaly using Z-score."""
        value = row.get(column)
        if value is None or column not in self.baseline_stats:
            return False
        
        stats = self.baseline_stats[column]
        if stats['std'] == 0:
            return False
        
        zscore = abs(float(value) - stats['mean']) / stats['std']
        
        if zscore > threshold:
            self.anomalies.append({
                'row': row,
                'column': column,
                'value': value,
                'method': 'zscore',
                'score': zscore,
                'threshold': threshold
            })
            return True
        
        return False
    
    def detect_iqr(self, row: Dict, column: str) -> bool:
        """Detect anomaly using IQR method."""
        value = row.get(column)
        if value is None or column not in self.baseline_stats:
            return False
        
        stats = self.baseline_stats[column]
        iqr = stats['q3'] - stats['q1']
        lower_bound = stats['q1'] - 1.5 * iqr
        upper_bound = stats['q3'] + 1.5 * iqr
        
        if float(value) < lower_bound or float(value) > upper_bound:
            self.anomalies.append({
                'row': row,
                'column': column,
                'value': value,
                'method': 'iqr',
                'bounds': [lower_bound, upper_bound]
            })
            return True
        
        return False
    
    def get_summary(self) -> Dict:
        """Get anomaly detection summary."""
        return {
            'total_anomalies': len(self.anomalies),
            'by_method': {}
        }


class DataQualityFramework:
    """Framework for data quality validation."""
    
    def __init__(self):
        self.rules = []
        self.anomaly_detector = AnomalyDetector()
        self.validation_results = []
    
    def add_rule(self, rule: DataQualityRule):
        """Add a validation rule."""
        self.rules.append(rule)
    
    def validate(self, data: List[Dict]) -> Dict:
        """Validate data against all rules."""
        logger.info(f"Validating {len(data)} records against {len(self.rules)} rules...")
        
        total_records = len(data)
        passed_records = 0
        
        for row in data:
            row_valid = True
            
            for rule in self.rules:
                if not rule.validate(row):
                    if rule.severity == 'error':
                        row_valid = False
            
            if row_valid:
                passed_records += 1
        
        results = {
            'total_records': total_records,
            'passed_records': passed_records,
            'failed_records': total_records - passed_records,
            'pass_rate': passed_records / total_records if total_records > 0 else 0,
            'rule_results': [rule.get_result() for rule in self.rules]
        }
        
        self.validation_results.append(results)
        
        return results
    
    def detect_anomalies(self, data: List[Dict], column: str, method: str = 'zscore') -> Dict:
        """Detect anomalies in data."""
        logger.info(f"Detecting anomalies in column '{column}' using {method}...")
        
        # Fit baseline
        self.anomaly_detector.fit(data, column)
        
        # Detect anomalies
        for row in data:
            if method == 'zscore':
                self.anomaly_detector.detect_zscore(row, column)
            elif method == 'iqr':
                self.anomaly_detector.detect_iqr(row, column)
        
        return self.anomaly_detector.get_summary()
    
    def generate_quality_report(self) -> Dict:
        """Generate comprehensive quality report."""
        return {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_rules': len(self.rules),
                'total_validations': len(self.validation_results)
            },
            'dimensions': {
                'completeness': 'Percentage of non-null values',
                'uniqueness': 'Percentage of unique values',
                'validity': 'Percentage of values meeting format requirements',
                'consistency': 'Percentage of values consistent across sources',
                'accuracy': 'Percentage of correct values',
                'timeliness': 'Percentage of up-to-date records'
            },
            'recommendations': [
                'Implement data quality monitoring dashboards',
                'Set up alerts for quality threshold breaches',
                'Establish data quality SLAs',
                'Create data quality scorecards',
                'Implement data lineage tracking'
            ]
        }


class DataQualityDemo:
    """Demonstrate data quality framework."""
    
    def __init__(self):
        self.framework = DataQualityFramework()
    
    def generate_sample_data(self, n: int = 100) -> List[Dict]:
        """Generate sample data with quality issues."""
        data = []
        
        for i in range(n):
            # Introduce some data quality issues
            has_issues = random.random() < 0.2  # 20% bad records
            
            record = {
                'id': i + 1,
                'email': f'user{i}@example.com' if not has_issues else 'invalid-email',
                'age': random.randint(18, 80) if not has_issues else random.choice([-5, 150]),
                'salary': random.randint(30000, 200000),
                'department': random.choice(['Engineering', 'Sales', 'Marketing']),
                'phone': f'555-{random.randint(1000, 9999)}' if not has_issues else '',
                'join_date': f'202{random.randint(0, 3)}-{random.randint(1, 12):02d}-15'
            }
            
            # Some nulls
            if random.random() < 0.1:
                record['email'] = None
            
            data.append(record)
        
        return data
    
    def run_quality_checks(self) -> Dict:
        """Run data quality checks."""
        # Generate data
        data = self.generate_sample_data(100)
        
        # Add rules
        self.framework.add_rule(CompletenessRule('email_required', 'email'))
        self.framework.add_rule(PatternRule('email_format', 'email', r'^[\w\.-]+@[\w\.-]+\.\w+$'))
        self.framework.add_rule(RangeRule('age_valid', 'age', 0, 120))
        self.framework.add_rule(CompletenessRule('phone_required', 'phone'))
        self.framework.add_rule(RangeRule('salary_valid', 'salary', 0, 1000000))
        
        # Validate
        validation_results = self.framework.validate(data)
        
        # Detect anomalies
        anomaly_results = self.framework.detect_anomalies(data, 'salary', method='zscore')
        
        # Generate report
        report = self.framework.generate_quality_report()
        
        return {
            'validation_results': validation_results,
            'anomaly_results': anomaly_results,
            'quality_report': report
        }
    
    def get_data_quality_best_practices(self) -> Dict:
        """Get data quality best practices."""
        return {
            'prevention': [
                'Implement schema validation at ingestion',
                'Use dropdowns instead of free text where possible',
                'Validate data at the source (client-side)',
                'Implement referential integrity constraints'
            ],
            'detection': [
                'Run automated quality checks daily',
                'Set up anomaly detection for key metrics',
                'Monitor data freshness and latency',
                'Compare against historical baselines'
            ],
            'correction': [
                'Establish data stewardship roles',
                'Create data quality issue triage process',
                'Implement automated data cleansing rules',
                'Maintain data quality scorecards'
            ],
            'monitoring': [
                'Dashboard for data quality metrics',
                'Alerts for critical quality issues',
                'Data lineage visualization',
                'Regular data quality audits'
            ]
        }
    
    def run_demo(self) -> Dict:
        """Run complete data quality demonstration."""
        quality_results = self.run_quality_checks()
        best_practices = self.get_data_quality_best_practices()
        
        logger.info("Task 29 completed successfully!")
        
        return {
            'quality_results': quality_results,
            'best_practices': best_practices
        }


def main():
    """Main execution for Task 29."""
    demo = DataQualityDemo()
    results = demo.run_demo()
    
    print("\n=== Data Quality: Validation & Anomaly Detection ===")
    
    print("\n1. Validation Results:")
    vr = results['quality_results']['validation_results']
    print(f"  Total Records: {vr['total_records']}")
    print(f"  Passed: {vr['passed_records']}")
    print(f"  Failed: {vr['failed_records']}")
    print(f"  Pass Rate: {vr['pass_rate']*100:.1f}%")
    
    print("\n2. Rule Results:")
    for rule in vr['rule_results']:
        print(f"  {rule['rule_name']}: {rule['violations']} violations")
    
    print("\n3. Anomalies Detected:")
    print(f"  Total: {results['quality_results']['anomaly_results']['total_anomalies']}")
    
    return results


if __name__ == "__main__":
    main()
