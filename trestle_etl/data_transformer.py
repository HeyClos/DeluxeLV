"""
Data Transformer for Trestle ETL Pipeline.

Handles field normalization, data type conversion, and validation
for transforming OData API responses into MySQL-compatible format.
"""

import re
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, List, Optional, Set, Union
from dataclasses import dataclass


class DataTransformationError(Exception):
    """Raised when data transformation fails."""
    pass


class ValidationError(Exception):
    """Raised when data validation fails."""
    pass


@dataclass
class ValidationResult:
    """Result of data validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]


@dataclass
class TransformationStats:
    """Statistics from data transformation process."""
    total_records: int
    valid_records: int
    invalid_records: int
    duplicates_detected: int
    field_transformations: Dict[str, int]
    validation_errors: List[str]


class DataTransformer:
    """
    Transforms OData API responses into MySQL-compatible format.
    
    Handles field name normalization, data type conversion, validation,
    and duplicate detection for real estate property data.
    """
    
    # MySQL reserved words to avoid as column names
    MYSQL_RESERVED_WORDS = {
        'add', 'all', 'alter', 'analyze', 'and', 'as', 'asc', 'asensitive',
        'before', 'between', 'bigint', 'binary', 'blob', 'both', 'by',
        'call', 'cascade', 'case', 'change', 'char', 'character', 'check',
        'collate', 'column', 'condition', 'constraint', 'continue', 'convert',
        'create', 'cross', 'current_date', 'current_time', 'current_timestamp',
        'current_user', 'cursor', 'database', 'databases', 'day_hour',
        'day_microsecond', 'day_minute', 'day_second', 'dec', 'decimal',
        'declare', 'default', 'delayed', 'delete', 'desc', 'describe',
        'deterministic', 'distinct', 'distinctrow', 'div', 'double', 'drop',
        'dual', 'each', 'else', 'elseif', 'enclosed', 'escaped', 'exists',
        'exit', 'explain', 'false', 'fetch', 'float', 'float4', 'float8',
        'for', 'force', 'foreign', 'from', 'fulltext', 'grant', 'group',
        'having', 'high_priority', 'hour_microsecond', 'hour_minute',
        'hour_second', 'if', 'ignore', 'in', 'index', 'infile', 'inner',
        'inout', 'insensitive', 'insert', 'int', 'int1', 'int2', 'int3',
        'int4', 'int8', 'integer', 'interval', 'into', 'is', 'iterate',
        'join', 'key', 'keys', 'kill', 'leading', 'leave', 'left', 'like',
        'limit', 'linear', 'lines', 'load', 'localtime', 'localtimestamp',
        'lock', 'long', 'longblob', 'longtext', 'loop', 'low_priority',
        'match', 'mediumblob', 'mediumint', 'mediumtext', 'middleint',
        'minute_microsecond', 'minute_second', 'mod', 'modifies', 'natural',
        'not', 'no_write_to_binlog', 'null', 'numeric', 'on', 'optimize',
        'option', 'optionally', 'or', 'order', 'out', 'outer', 'outfile',
        'precision', 'primary', 'procedure', 'purge', 'range', 'read',
        'reads', 'real', 'references', 'regexp', 'release', 'rename',
        'repeat', 'replace', 'require', 'restrict', 'return', 'revoke',
        'right', 'rlike', 'schema', 'schemas', 'second_microsecond', 'select',
        'sensitive', 'separator', 'set', 'show', 'smallint', 'spatial',
        'specific', 'sql', 'sqlexception', 'sqlstate', 'sqlwarning',
        'sql_big_result', 'sql_calc_found_rows', 'sql_small_result', 'ssl',
        'starting', 'straight_join', 'table', 'terminated', 'then', 'tinyblob',
        'tinyint', 'tinytext', 'to', 'trailing', 'trigger', 'true', 'undo',
        'union', 'unique', 'unlock', 'unsigned', 'update', 'usage', 'use',
        'using', 'utc_date', 'utc_time', 'utc_timestamp', 'values', 'varbinary',
        'varchar', 'varcharacter', 'varying', 'when', 'where', 'while',
        'with', 'write', 'x509', 'xor', 'year_month', 'zerofill'
    }
    
    # Required fields for property records
    REQUIRED_FIELDS = {
        'listing_key': str,
        'modification_timestamp': datetime
    }
    
    # Field type mappings for validation
    FIELD_TYPE_MAPPINGS = {
        'listing_key': str,
        'list_price': (int, float, Decimal, type(None)),
        'property_type': (str, type(None)),
        'bedrooms_total': (int, type(None)),
        'bathrooms_total': (int, float, Decimal, type(None)),
        'square_feet': (int, type(None)),
        'lot_size_acres': (int, float, Decimal, type(None)),
        'year_built': (int, type(None)),
        'listing_status': (str, type(None)),
        'modification_timestamp': datetime,
        'street_address': (str, type(None)),
        'city': (str, type(None)),
        'state_or_province': (str, type(None)),
        'postal_code': (str, type(None))
    }
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize DataTransformer.
        
        Args:
            logger: Optional logger for recording transformation events.
        """
        self.logger = logger or logging.getLogger(__name__)
        self._field_name_cache: Dict[str, str] = {}
        self._duplicate_keys: Set[str] = set()
        
    def normalize_field_name(self, field_name: str) -> str:
        """
        Normalize API field name to MySQL column name.
        
        Converts field names to lowercase with underscores, handles
        MySQL reserved words, and ensures valid SQL identifiers.
        
        Args:
            field_name: Original field name from API.
            
        Returns:
            Normalized field name suitable for MySQL column.
            
        Raises:
            DataTransformationError: If field name cannot be normalized.
        """
        if field_name is None or not isinstance(field_name, str):
            raise DataTransformationError(f"Invalid field name: {field_name}")
        
        # Check for empty or whitespace-only strings
        if not field_name.strip():
            raise DataTransformationError(f"Empty or whitespace-only field name: '{field_name}'")
        
        # Check cache first
        if field_name in self._field_name_cache:
            return self._field_name_cache[field_name]
        
        # Convert to lowercase and replace non-alphanumeric with underscores
        normalized = re.sub(r'[^a-zA-Z0-9_]', '_', field_name.lower())
        
        # Remove multiple consecutive underscores
        normalized = re.sub(r'_+', '_', normalized)
        
        # Remove leading/trailing underscores
        normalized = normalized.strip('_')
        
        # Ensure it starts with a letter or underscore
        if normalized and not re.match(r'^[a-zA-Z_]', normalized):
            normalized = f'field_{normalized}'
        
        # Handle empty result after cleaning
        if not normalized:
            normalized = 'field_unknown'
        
        # Handle MySQL reserved words
        if normalized.lower() in self.MYSQL_RESERVED_WORDS:
            normalized = f'{normalized}_field'
        
        # Ensure maximum length (MySQL column names can be up to 64 characters)
        if len(normalized) > 64:
            # Truncate more aggressively to ensure we stay under 64 chars
            if normalized.endswith('_field'):
                # Handle reserved word suffix
                base_length = 64 - len('_field')
                normalized = normalized[:base_length] + '_field'
            else:
                # Regular truncation
                normalized = normalized[:64]
        
        # Final validation that we have a valid result
        if len(normalized) > 64:
            # Emergency truncation if somehow we're still over
            normalized = normalized[:64]
        
        # Cache the result
        self._field_name_cache[field_name] = normalized
        
        return normalized
    
    def convert_data_type(self, value: Any, target_type: str, field_name: str = "") -> Any:
        """
        Convert API data type to MySQL-compatible format.
        
        Args:
            value: Value to convert.
            target_type: Target data type ('string', 'integer', 'decimal', 'datetime', 'boolean').
            field_name: Field name for error reporting.
            
        Returns:
            Converted value.
            
        Raises:
            DataTransformationError: If conversion fails.
        """
        if value is None:
            return None
        
        try:
            if target_type == 'string':
                if isinstance(value, str):
                    # Trim whitespace and handle empty strings
                    result = value.strip()
                    return result if result else None
                else:
                    return str(value)
            
            elif target_type == 'integer':
                if isinstance(value, int):
                    return value
                elif isinstance(value, (float, Decimal)):
                    # Convert to int, but check for precision loss
                    int_val = int(value)
                    if abs(float(value) - int_val) > 0.001:  # Allow small floating point errors
                        self.logger.warning(f"Precision loss converting {field_name}: {value} -> {int_val}")
                    return int_val
                elif isinstance(value, str):
                    # Try to parse string as integer
                    cleaned = value.strip().replace(',', '')  # Remove commas
                    if cleaned:
                        return int(float(cleaned))  # Parse as float first to handle "123.0"
                    else:
                        return None
                else:
                    return int(value)
            
            elif target_type == 'decimal':
                if isinstance(value, Decimal):
                    return value
                elif isinstance(value, (int, float)):
                    return Decimal(str(value))
                elif isinstance(value, str):
                    cleaned = value.strip().replace(',', '').replace('$', '')  # Remove commas and dollar signs
                    if cleaned:
                        return Decimal(cleaned)
                    else:
                        return None
                else:
                    return Decimal(str(value))
            
            elif target_type == 'datetime':
                if isinstance(value, datetime):
                    return value
                elif isinstance(value, str):
                    # Try common datetime formats
                    formats = [
                        '%Y-%m-%dT%H:%M:%S.%fZ',  # ISO format with microseconds
                        '%Y-%m-%dT%H:%M:%SZ',     # ISO format
                        '%Y-%m-%dT%H:%M:%S',      # ISO format without Z
                        '%Y-%m-%d %H:%M:%S',      # SQL format
                        '%Y-%m-%d',               # Date only
                        '%m/%d/%Y',               # US date format
                        '%m/%d/%Y %H:%M:%S'       # US datetime format
                    ]
                    
                    for fmt in formats:
                        try:
                            return datetime.strptime(value, fmt)
                        except ValueError:
                            continue
                    
                    raise DataTransformationError(f"Unable to parse datetime: {value}")
                else:
                    raise DataTransformationError(f"Cannot convert {type(value)} to datetime")
            
            elif target_type == 'boolean':
                if isinstance(value, bool):
                    return value
                elif isinstance(value, str):
                    lower_val = value.lower().strip()
                    if lower_val in ('true', '1', 'yes', 'y', 'on'):
                        return True
                    elif lower_val in ('false', '0', 'no', 'n', 'off'):
                        return False
                    else:
                        raise DataTransformationError(f"Cannot convert string to boolean: {value}")
                elif isinstance(value, (int, float)):
                    return bool(value)
                else:
                    return bool(value)
            
            else:
                raise DataTransformationError(f"Unknown target type: {target_type}")
                
        except (ValueError, InvalidOperation, TypeError) as e:
            raise DataTransformationError(
                f"Failed to convert {field_name} value '{value}' to {target_type}: {str(e)}"
            )
    
    def validate_required_fields(self, record: Dict[str, Any]) -> ValidationResult:
        """
        Validate that required fields are present and properly formatted.
        
        Args:
            record: Record to validate.
            
        Returns:
            ValidationResult with validation status and any errors.
        """
        errors = []
        warnings = []
        
        # Check required fields
        for field_name, expected_type in self.REQUIRED_FIELDS.items():
            if field_name not in record:
                errors.append(f"Missing required field: {field_name}")
            else:
                value = record[field_name]
                if value is None:
                    errors.append(f"Required field {field_name} is null")
                elif not isinstance(value, expected_type):
                    errors.append(f"Field {field_name} has wrong type: expected {expected_type.__name__}, got {type(value).__name__}")
        
        # Validate field types for optional fields
        for field_name, value in record.items():
            if field_name in self.FIELD_TYPE_MAPPINGS:
                expected_types = self.FIELD_TYPE_MAPPINGS[field_name]
                if not isinstance(expected_types, tuple):
                    expected_types = (expected_types,)
                
                if value is not None and not isinstance(value, expected_types):
                    warnings.append(f"Field {field_name} has unexpected type: expected {[t.__name__ for t in expected_types]}, got {type(value).__name__}")
        
        # Validate specific business rules
        if 'list_price' in record and record['list_price'] is not None:
            try:
                price = float(record['list_price'])
                if price < 0:
                    errors.append("List price cannot be negative")
                elif price > 1000000000:  # $1 billion seems like a reasonable upper limit
                    warnings.append(f"List price seems unusually high: ${price:,.2f}")
            except (ValueError, TypeError):
                errors.append(f"Invalid list price format: {record['list_price']}")
        
        if 'year_built' in record and record['year_built'] is not None:
            try:
                year = int(record['year_built'])
                current_year = datetime.now().year
                if year < 1800:
                    errors.append(f"Year built too early: {year}")
                elif year > current_year + 5:  # Allow some future construction
                    errors.append(f"Year built too far in future: {year}")
            except (ValueError, TypeError):
                errors.append(f"Invalid year built format: {record['year_built']}")
        
        if 'bedrooms_total' in record and record['bedrooms_total'] is not None:
            try:
                bedrooms = int(record['bedrooms_total'])
                if bedrooms < 0:
                    errors.append("Bedrooms cannot be negative")
                elif bedrooms > 50:  # Reasonable upper limit
                    warnings.append(f"Unusually high bedroom count: {bedrooms}")
            except (ValueError, TypeError):
                errors.append(f"Invalid bedrooms format: {record['bedrooms_total']}")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
    
    def detect_duplicate(self, record: Dict[str, Any], existing_keys: Optional[Set[str]] = None) -> bool:
        """
        Detect if record is a duplicate based on ListingKey.
        
        Args:
            record: Record to check for duplication.
            existing_keys: Set of existing listing keys to check against.
            
        Returns:
            True if record is a duplicate, False otherwise.
        """
        if 'listing_key' not in record or record['listing_key'] is None:
            return False
        
        listing_key = str(record['listing_key'])
        
        # Check against provided existing keys
        if existing_keys and listing_key in existing_keys:
            return True
        
        # Check against internal duplicate tracking
        if listing_key in self._duplicate_keys:
            return True
        
        # Add to internal tracking
        self._duplicate_keys.add(listing_key)
        return False
    
    def transform_record(self, api_record: Dict[str, Any], existing_keys: Optional[Set[str]] = None) -> Dict[str, Any]:
        """
        Transform a single API record to MySQL format.
        
        Args:
            api_record: Raw record from API.
            existing_keys: Set of existing listing keys for duplicate detection.
            
        Returns:
            Transformed record ready for database insertion.
            
        Raises:
            DataTransformationError: If transformation fails.
            ValidationError: If record validation fails.
        """
        if not isinstance(api_record, dict):
            raise DataTransformationError(f"Expected dict, got {type(api_record)}")
        
        transformed = {}
        
        # Transform field names and values
        for api_field, value in api_record.items():
            # Skip metadata fields
            if api_field.startswith('@') or api_field.startswith('_'):
                continue
            
            # Normalize field name
            try:
                db_field = self.normalize_field_name(api_field)
            except DataTransformationError as e:
                self.logger.warning(f"Skipping field {api_field}: {str(e)}")
                continue
            
            # Convert data types based on field mappings
            if db_field in self.FIELD_TYPE_MAPPINGS:
                expected_types = self.FIELD_TYPE_MAPPINGS[db_field]
                if not isinstance(expected_types, tuple):
                    expected_types = (expected_types,)
                
                # Determine target type for conversion
                if datetime in expected_types:
                    target_type = 'datetime'
                elif Decimal in expected_types or float in expected_types:
                    target_type = 'decimal'
                elif int in expected_types:
                    target_type = 'integer'
                elif bool in expected_types:
                    target_type = 'boolean'
                else:
                    target_type = 'string'
                
                try:
                    transformed[db_field] = self.convert_data_type(value, target_type, api_field)
                except DataTransformationError as e:
                    self.logger.warning(f"Data conversion failed for {api_field}: {str(e)}")
                    # For non-required fields, set to None on conversion failure
                    if db_field not in self.REQUIRED_FIELDS:
                        transformed[db_field] = None
                    else:
                        raise ValidationError(f"Required field conversion failed: {str(e)}")
            else:
                # Unknown field, convert to string
                try:
                    transformed[db_field] = self.convert_data_type(value, 'string', api_field)
                except DataTransformationError:
                    # Skip fields that can't be converted
                    self.logger.warning(f"Skipping unconvertible field {api_field}: {value}")
                    continue
        
        # Validate the transformed record
        validation_result = self.validate_required_fields(transformed)
        if not validation_result.is_valid:
            raise ValidationError(f"Record validation failed: {'; '.join(validation_result.errors)}")
        
        # Log warnings
        for warning in validation_result.warnings:
            self.logger.warning(f"Record validation warning: {warning}")
        
        # Check for duplicates
        is_duplicate = self.detect_duplicate(transformed, existing_keys)
        transformed['_is_duplicate'] = is_duplicate
        
        return transformed
    
    def transform_batch(
        self, 
        api_records: List[Dict[str, Any]], 
        existing_keys: Optional[Set[str]] = None,
        continue_on_error: bool = True
    ) -> Dict[str, Any]:
        """
        Transform a batch of API records.
        
        Args:
            api_records: List of raw records from API.
            existing_keys: Set of existing listing keys for duplicate detection.
            continue_on_error: Whether to continue processing on individual record errors.
            
        Returns:
            Dictionary with transformed records and statistics.
        """
        transformed_records = []
        validation_errors = []
        field_transformations = {}
        
        # Clear duplicate cache at start of batch to ensure clean state
        self.clear_duplicate_cache()
        
        for i, record in enumerate(api_records):
            try:
                transformed = self.transform_record(record, existing_keys)
                transformed_records.append(transformed)
                
                # Track field transformations
                for field in transformed.keys():
                    if not field.startswith('_'):
                        field_transformations[field] = field_transformations.get(field, 0) + 1
                        
            except (DataTransformationError, ValidationError) as e:
                error_msg = f"Record {i}: {str(e)}"
                validation_errors.append(error_msg)
                self.logger.error(error_msg)
                
                if not continue_on_error:
                    raise DataTransformationError(f"Batch transformation failed: {error_msg}")
        
        # Calculate statistics
        total_records = len(api_records)
        valid_records = len(transformed_records)
        invalid_records = total_records - valid_records
        duplicates_detected = sum(1 for r in transformed_records if r.get('_is_duplicate', False))
        
        stats = TransformationStats(
            total_records=total_records,
            valid_records=valid_records,
            invalid_records=invalid_records,
            duplicates_detected=duplicates_detected,
            field_transformations=field_transformations,
            validation_errors=validation_errors
        )
        
        return {
            'records': transformed_records,
            'stats': stats
        }
    
    def clear_duplicate_cache(self) -> None:
        """Clear the internal duplicate key cache."""
        self._duplicate_keys.clear()
    
    def get_field_mapping(self) -> Dict[str, str]:
        """
        Get the current field name mapping cache.
        
        Returns:
            Dictionary mapping API field names to database column names.
        """
        return self._field_name_cache.copy()