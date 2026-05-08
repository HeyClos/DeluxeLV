"""
Data Transformer for Trestle ETL Pipeline.

Handles data type conversion, validation, and duplicate detection
for transforming OData API responses into MySQL-compatible format.

Field names from the API are preserved as-is (no CamelCase→snake_case conversion).
"""

import math
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

    Handles data type conversion, validation, and duplicate detection
    for real estate property data. Field names are preserved as they
    arrive from the API.
    """

    # Required fields for property records (using API field names)
    REQUIRED_FIELDS = {
        'ListingKey': str,
        'ModificationTimestamp': datetime
    }

    # Field type mappings for validation (using API field names)
    FIELD_TYPE_MAPPINGS = {
        'ListingKey': str,
        'ListPrice': (int, float, Decimal, type(None)),
        'PropertyType': (str, type(None)),
        'BedroomsTotal': (int, type(None)),
        'BathroomsTotalInteger': (int, float, Decimal, type(None)),
        'LivingArea': (int, type(None)),
        'LotSizeAcres': (int, float, Decimal, type(None)),
        'YearBuilt': (int, type(None)),
        'StandardStatus': (str, type(None)),
        'ModificationTimestamp': datetime,
        'StreetNumber': (str, type(None)),
        'StreetName': (str, type(None)),
        'City': (str, type(None)),
        'StateOrProvince': (str, type(None)),
        'PostalCode': (str, type(None))
    }

    # Maximum number of entries in the duplicate key tracker before eviction
    MAX_DUPLICATE_KEYS_SIZE = 1_000_000

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize DataTransformer.

        Args:
            logger: Optional logger for recording transformation events.
        """
        self.logger = logger or logging.getLogger(__name__)
        self._duplicate_keys: Set[str] = set()

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
                    float_val = float(value)
                    if not math.isfinite(float_val):
                        raise DataTransformationError(f"Cannot convert non-finite value to integer: {value}")
                    # Convert to int, but check for precision loss
                    int_val = int(value)
                    if abs(float_val - int_val) > 0.001:  # Allow small floating point errors
                        self.logger.warning(f"Precision loss converting {field_name}: {value} -> {int_val}")
                    return int_val
                elif isinstance(value, str):
                    # Try to parse string as integer
                    cleaned = value.strip().replace(',', '')  # Remove commas
                    if cleaned:
                        float_val = float(cleaned)
                        if not math.isfinite(float_val):
                            raise DataTransformationError(f"Cannot convert non-finite value to integer: {value}")
                        return int(float_val)  # Parse as float first to handle "123.0"
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
                    # Try Python's built-in ISO 8601 parser first (handles timezone offsets)
                    try:
                        dt = datetime.fromisoformat(value)
                        # Strip timezone info for MySQL DATETIME compatibility
                        return dt.replace(tzinfo=None)
                    except (ValueError, TypeError):
                        pass

                    # Fall back to manual format parsing
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
        if 'ListPrice' in record and record['ListPrice'] is not None:
            try:
                price = float(record['ListPrice'])
                if price < 0:
                    errors.append("List price cannot be negative")
                elif price > 1000000000:  # $1 billion seems like a reasonable upper limit
                    warnings.append(f"List price seems unusually high: ${price:,.2f}")
            except (ValueError, TypeError):
                errors.append(f"Invalid list price format: {record['ListPrice']}")

        if 'YearBuilt' in record and record['YearBuilt'] is not None:
            try:
                year = int(record['YearBuilt'])
                current_year = datetime.now().year
                if year < 1800:
                    errors.append(f"Year built too early: {year}")
                elif year > current_year + 5:  # Allow some future construction
                    errors.append(f"Year built too far in future: {year}")
            except (ValueError, TypeError):
                errors.append(f"Invalid year built format: {record['YearBuilt']}")

        if 'BedroomsTotal' in record and record['BedroomsTotal'] is not None:
            try:
                bedrooms = int(record['BedroomsTotal'])
                if bedrooms < 0:
                    errors.append("Bedrooms cannot be negative")
                elif bedrooms > 50:  # Reasonable upper limit
                    warnings.append(f"Unusually high bedroom count: {bedrooms}")
            except (ValueError, TypeError):
                errors.append(f"Invalid bedrooms format: {record['BedroomsTotal']}")

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
        if 'ListingKey' not in record or record['ListingKey'] is None:
            return False

        listing_key = str(record['ListingKey'])

        # Check against provided existing keys
        if existing_keys and listing_key in existing_keys:
            return True

        # Check against internal duplicate tracking
        if listing_key in self._duplicate_keys:
            return True

        # Add to internal tracking (with eviction to prevent unbounded growth)
        if len(self._duplicate_keys) >= self.MAX_DUPLICATE_KEYS_SIZE:
            self.logger.warning(
                f"Duplicate key cache reached {self.MAX_DUPLICATE_KEYS_SIZE} entries, clearing"
            )
            self._duplicate_keys.clear()
        self._duplicate_keys.add(listing_key)
        return False

    def transform_record(self, api_record: Dict[str, Any], existing_keys: Optional[Set[str]] = None) -> Dict[str, Any]:
        """
        Transform a single API record to MySQL format.

        Field names are preserved as-is from the API. Only data type
        conversion, validation, and metadata stripping are performed.

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

        # Transform field values (preserve field names as-is)
        for api_field, value in api_record.items():
            # Skip OData metadata fields
            if api_field.startswith('@') or api_field.startswith('_'):
                continue

            # Convert data types based on field mappings
            if api_field in self.FIELD_TYPE_MAPPINGS:
                expected_types = self.FIELD_TYPE_MAPPINGS[api_field]
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
                    transformed[api_field] = self.convert_data_type(value, target_type, api_field)
                except DataTransformationError as e:
                    self.logger.warning(f"Data conversion failed for {api_field}: {str(e)}")
                    # For non-required fields, set to None on conversion failure
                    if api_field not in self.REQUIRED_FIELDS:
                        transformed[api_field] = None
                    else:
                        raise ValidationError(f"Required field conversion failed: {str(e)}")
            else:
                # Unknown field, convert to string
                try:
                    transformed[api_field] = self.convert_data_type(value, 'string', api_field)
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
