#!/usr/bin/env python3
"""
Task 4: Advanced Python
Create reusable modules for data transformation, including functions for normalization, aggregation, and validation.
"""

import json
import re
import logging
from typing import List, Dict, Any, Callable, Optional, Union
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from functools import wraps
import hashlib
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Decorator for logging transformations
def log_transformation(func: Callable) -> Callable:
    """Decorator to log data transformations."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Starting transformation: {func.__name__}")
        start_time = datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Completed transformation: {func.__name__} in {duration:.4f}s")
        return result
    return wrapper


# Decorator for validation
def validate_input(validator: Callable) -> Callable:
    """Decorator to validate input before processing."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(data: Any, *args, **kwargs):
            is_valid, error_msg = validator(data)
            if not is_valid:
                raise ValueError(f"Validation failed: {error_msg}")
            return func(data, *args, **kwargs)
        return wrapper
    return decorator


@dataclass
class TransformationResult:
    """Result container for data transformations."""
    success: bool
    data: Any = None
    error: str = None
    metadata: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        return {
            'success': self.success,
            'data': self.data,
            'error': self.error,
            'metadata': self.metadata
        }


class DataTransformer(ABC):
    """Abstract base class for data transformers."""
    
    @abstractmethod
    def transform(self, data: Any) -> TransformationResult:
        pass
    
    @abstractmethod
    def validate(self, data: Any) -> tuple[bool, str]:
        pass


class NormalizationTransformer(DataTransformer):
    """Normalize numeric data using various techniques."""
    
    def __init__(self, method: str = 'min_max', feature_range: tuple = (0, 1)):
        self.method = method
        self.feature_range = feature_range
        self.stats = {}
    
    def validate(self, data: Any) -> tuple[bool, str]:
        if not isinstance(data, (list, dict)):
            return False, "Data must be a list or dictionary"
        if len(data) == 0:
            return False, "Data cannot be empty"
        return True, ""
    
    @validate_input(lambda d: (isinstance(d, (list, dict)) and len(d) > 0, "Invalid data"))
    @log_transformation
    def transform(self, data: Union[List[float], Dict[str, float]]) -> TransformationResult:
        """Normalize data using specified method."""
        try:
            if isinstance(data, dict):
                values = list(data.values())
                keys = list(data.keys())
            else:
                values = data
                keys = None
            
            if self.method == 'min_max':
                normalized = self._min_max_normalize(values)
            elif self.method == 'z_score':
                normalized = self._z_score_normalize(values)
            elif self.method == 'decimal_scaling':
                normalized = self._decimal_scaling(values)
            else:
                return TransformationResult(False, error=f"Unknown method: {self.method}")
            
            if keys:
                result = dict(zip(keys, normalized))
            else:
                result = normalized
            
            return TransformationResult(
                success=True,
                data=result,
                metadata={
                    'method': self.method,
                    'original_range': (min(values), max(values)),
                    'normalized_range': (min(normalized), max(normalized))
                }
            )
        except Exception as e:
            return TransformationResult(False, error=str(e))
    
    def _min_max_normalize(self, values: List[float]) -> List[float]:
        """Min-Max normalization to feature range."""
        min_val = min(values)
        max_val = max(values)
        range_val = max_val - min_val
        
        if range_val == 0:
            return [self.feature_range[0]] * len(values)
        
        self.stats = {'min': min_val, 'max': max_val}
        
        return [
            self.feature_range[0] + (v - min_val) / range_val * 
            (self.feature_range[1] - self.feature_range[0])
            for v in values
        ]
    
    def _z_score_normalize(self, values: List[float]) -> List[float]:
        """Z-score normalization (standardization)."""
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        std = variance ** 0.5
        
        self.stats = {'mean': mean, 'std': std}
        
        if std == 0:
            return [0] * len(values)
        
        return [(v - mean) / std for v in values]
    
    def _decimal_scaling(self, values: List[float]) -> List[float]:
        """Decimal scaling normalization."""
        max_abs = max(abs(v) for v in values)
        if max_abs == 0:
            return values
        
        digits = len(str(int(max_abs)))
        divisor = 10 ** digits
        
        return [v / divisor for v in values]


class AggregationTransformer(DataTransformer):
    """Aggregate data using various functions."""
    
    AGG_FUNCTIONS = ['sum', 'avg', 'min', 'max', 'count', 'std', 'var']
    
    def __init__(self, group_by: Optional[str] = None, 
                 aggregations: Optional[Dict[str, str]] = None):
        self.group_by = group_by
        self.aggregations = aggregations or {}
    
    def validate(self, data: Any) -> tuple[bool, str]:
        if not isinstance(data, list):
            return False, "Data must be a list of records"
        if len(data) == 0:
            return False, "Data cannot be empty"
        if not all(isinstance(d, dict) for d in data):
            return False, "All records must be dictionaries"
        return True, ""
    
    @validate_input(lambda d: (isinstance(d, list) and len(d) > 0 and all(isinstance(x, dict) for x in d), "Invalid data"))
    @log_transformation
    def transform(self, data: List[Dict]) -> TransformationResult:
        """Aggregate data based on configuration."""
        try:
            if self.group_by:
                result = self._grouped_aggregation(data)
            else:
                result = self._simple_aggregation(data)
            
            return TransformationResult(
                success=True,
                data=result,
                metadata={
                    'group_by': self.group_by,
                    'aggregations': self.aggregations,
                    'input_count': len(data)
                }
            )
        except Exception as e:
            return TransformationResult(False, error=str(e))
    
    def _simple_aggregation(self, data: List[Dict]) -> Dict[str, Any]:
        """Perform simple aggregation without grouping."""
        result = {}
        
        for field, func in self.aggregations.items():
            values = [d.get(field) for d in data if d.get(field) is not None]
            values = [v for v in values if isinstance(v, (int, float))]
            
            if func == 'sum':
                result[f"{field}_sum"] = sum(values)
            elif func == 'avg':
                result[f"{field}_avg"] = sum(values) / len(values) if values else 0
            elif func == 'min':
                result[f"{field}_min"] = min(values) if values else None
            elif func == 'max':
                result[f"{field}_max"] = max(values) if values else None
            elif func == 'count':
                result[f"{field}_count"] = len(values)
            elif func == 'std':
                result[f"{field}_std"] = self._calculate_std(values)
            elif func == 'var':
                result[f"{field}_var"] = self._calculate_var(values)
        
        return result
    
    def _grouped_aggregation(self, data: List[Dict]) -> Dict[str, Dict]:
        """Perform grouped aggregation."""
        groups = {}
        
        for record in data:
            key = str(record.get(self.group_by, 'unknown'))
            if key not in groups:
                groups[key] = []
            groups[key].append(record)
        
        result = {}
        for key, group_data in groups.items():
            agg_result = self._simple_aggregation(group_data)
            result[key] = agg_result
        
        return result
    
    def _calculate_std(self, values: List[float]) -> float:
        """Calculate standard deviation."""
        if len(values) < 2:
            return 0
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
        return variance ** 0.5
    
    def _calculate_var(self, values: List[float]) -> float:
        """Calculate variance."""
        if len(values) < 2:
            return 0
        mean = sum(values) / len(values)
        return sum((v - mean) ** 2 for v in values) / (len(values) - 1)


class ValidationTransformer(DataTransformer):
    """Validate data against various rules."""
    
    def __init__(self, rules: Optional[Dict[str, List[Dict]]] = None):
        self.rules = rules or {}
        self.validation_errors = []
    
    def validate(self, data: Any) -> tuple[bool, str]:
        return True, ""  # Validation is the transformation itself
    
    @log_transformation
    def transform(self, data: List[Dict]) -> TransformationResult:
        """Validate data and return results."""
        valid_records = []
        invalid_records = []
        
        for idx, record in enumerate(data):
            errors = self._validate_record(record)
            if errors:
                invalid_records.append({
                    'index': idx,
                    'record': record,
                    'errors': errors
                })
            else:
                valid_records.append(record)
        
        return TransformationResult(
            success=True,
            data={
                'valid': valid_records,
                'invalid': invalid_records
            },
            metadata={
                'total_records': len(data),
                'valid_count': len(valid_records),
                'invalid_count': len(invalid_records),
                'validation_rate': len(valid_records) / len(data) if data else 0
            }
        )
    
    def _validate_record(self, record: Dict) -> List[str]:
        """Validate a single record against rules."""
        errors = []
        
        for field, field_rules in self.rules.items():
            value = record.get(field)
            
            for rule in field_rules:
                rule_type = rule.get('type')
                
                if rule_type == 'required' and (value is None or value == ''):
                    errors.append(f"{field}: Required field is missing")
                
                elif rule_type == 'type' and value is not None:
                    expected_type = rule.get('value')
                    if expected_type == 'int' and not isinstance(value, int):
                        errors.append(f"{field}: Expected int, got {type(value).__name__}")
                    elif expected_type == 'float' and not isinstance(value, (int, float)):
                        errors.append(f"{field}: Expected float, got {type(value).__name__}")
                    elif expected_type == 'str' and not isinstance(value, str):
                        errors.append(f"{field}: Expected string, got {type(value).__name__}")
                
                elif rule_type == 'range' and value is not None:
                    min_val = rule.get('min')
                    max_val = rule.get('max')
                    if min_val is not None and value < min_val:
                        errors.append(f"{field}: Value {value} is below minimum {min_val}")
                    if max_val is not None and value > max_val:
                        errors.append(f"{field}: Value {value} exceeds maximum {max_val}")
                
                elif rule_type == 'pattern' and value is not None:
                    pattern = rule.get('value')
                    if not re.match(pattern, str(value)):
                        errors.append(f"{field}: Value does not match pattern {pattern}")
                
                elif rule_type == 'enum' and value is not None:
                    allowed = rule.get('values', [])
                    if value not in allowed:
                        errors.append(f"{field}: Value must be one of {allowed}")
        
        return errors


class DataTransformationPipeline:
    """Pipeline for chaining multiple transformations."""
    
    def __init__(self):
        self.transformers: List[DataTransformer] = []
        self.results: List[TransformationResult] = []
    
    def add_transformer(self, transformer: DataTransformer) -> 'DataTransformationPipeline':
        """Add a transformer to the pipeline."""
        self.transformers.append(transformer)
        return self
    
    def execute(self, data: Any) -> TransformationResult:
        """Execute all transformers in sequence."""
        current_data = data
        
        for transformer in self.transformers:
            result = transformer.transform(current_data)
            self.results.append(result)
            
            if not result.success:
                return TransformationResult(
                    success=False,
                    error=f"Pipeline failed at {transformer.__class__.__name__}: {result.error}",
                    metadata={'failed_step': len(self.results)}
                )
            
            current_data = result.data
        
        return TransformationResult(
            success=True,
            data=current_data,
            metadata={
                'steps_executed': len(self.transformers),
                'step_results': [r.to_dict() for r in self.results]
            }
        )


def main():
    """Main execution for Task 4."""
    # Sample resume data
    resume_scores = [
        {'candidate_id': 'C001', 'skill_score': 85, 'experience_score': 70, 'education_score': 90},
        {'candidate_id': 'C002', 'skill_score': 75, 'experience_score': 85, 'education_score': 80},
        {'candidate_id': 'C003', 'skill_score': 90, 'experience_score': 60, 'education_score': 85},
        {'candidate_id': 'C004', 'skill_score': 65, 'experience_score': 75, 'education_score': 70},
        {'candidate_id': 'C005', 'skill_score': 80, 'experience_score': 80, 'education_score': 75},
    ]
    
    # Create pipeline
    pipeline = DataTransformationPipeline()
    
    # Add normalization transformer
    normalizer = NormalizationTransformer(method='min_max', feature_range=(0, 100))
    pipeline.add_transformer(normalizer)
    
    # Add aggregation transformer
    aggregator = AggregationTransformer(aggregations={
        'skill_score': 'avg',
        'experience_score': 'avg',
        'education_score': 'avg'
    })
    pipeline.add_transformer(aggregator)
    
    # Execute pipeline
    result = pipeline.execute(resume_scores)
    
    logger.info("Task 4 completed successfully!")
    
    print("\n=== Transformation Pipeline Result ===")
    print(json.dumps(result.to_dict(), indent=2))
    
    return result


if __name__ == "__main__":
    main()
