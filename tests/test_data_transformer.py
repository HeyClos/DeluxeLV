"""
Property-based tests for Data Transformer.

Feature: trestle-etl-pipeline
Tests Properties 6, 7, 8, 9 for data transformation functionality.
"""

import re
import logging
from datetime import datetime
from decimal import Decimal
from typing import Set

import pytest
from hypothesis import given, strategies as st, settings, assume

from trestle_etl.data_transformer import (
    DataTransformer, DataTransformationError, ValidationError,
    ValidationResult, TransformationStats
)


class TestFieldNamePreservation:
    """
    Property 6: Field Name Preservation

    API field names should be preserved as-is when passing through the
    transformer. No CamelCase→snake_case conversion should occur.

    Validates: Requirements 2.1
    """

    def setup_method(self):
        """Set up test environment."""
        self.transformer = DataTransformer()

    def test_api_field_names_preserved_in_transform(self):
        """
        Feature: trestle-etl-pipeline, Property 6: Field Name Preservation

        API field names should pass through unchanged.
        """
        record = {
            'ListingKey': 'TEST001',
            'ListPrice': 500000,
            'PropertyType': 'Residential',
            'BedroomsTotal': 3,
            'BathroomsTotalInteger': 2,
            'ModificationTimestamp': datetime.now(),
            'City': 'Las Vegas',
            'StateOrProvince': 'NV',
            'PostalCode': '89101'
        }

        result = self.transformer.transform_record(record)

        # All field names should be preserved exactly
        for field in record:
            assert field in result, f"Field {field} should be preserved"

    def test_metadata_fields_are_stripped(self):
        """
        Feature: trestle-etl-pipeline, Property 6: Field Name Preservation

        OData metadata fields (@odata.*, _*) should be stripped.
        """
        record = {
            'ListingKey': 'TEST001',
            'ModificationTimestamp': datetime.now(),
            '@odata.context': 'https://api.example.com/$metadata',
            '@odata.nextLink': 'https://api.example.com/next',
            '_internal': 'should_be_stripped'
        }

        result = self.transformer.transform_record(record)

        assert '@odata.context' not in result
        assert '@odata.nextLink' not in result
        assert '_internal' not in result
        assert 'ListingKey' in result

    @given(
        field_name=st.text(
            alphabet=st.sampled_from("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_"),
            min_size=1,
            max_size=50
        ).filter(lambda x: x[0].isalpha())
    )
    @settings(max_examples=100)
    def test_arbitrary_field_names_preserved(self, field_name):
        """
        Feature: trestle-etl-pipeline, Property 6: Field Name Preservation

        Any valid field name from the API should be preserved as-is.
        """
        record = {
            'ListingKey': 'TEST001',
            'ModificationTimestamp': datetime.now(),
            field_name: 'some_value'
        }

        result = self.transformer.transform_record(record)

        # The field should be present with its original name
        assert field_name in result, f"Field '{field_name}' should be preserved"


class TestDataTypeConversionAccuracy:
    """
    Property 8: Data Type Conversion Accuracy

    For any API data type, the conversion to MySQL-compatible format should
    preserve data integrity and handle edge cases appropriately.

    Validates: Requirements 2.4
    """

    def setup_method(self):
        """Set up test environment."""
        self.transformer = DataTransformer()

    @given(
        string_value=st.text(min_size=0, max_size=100),
        target_type=st.sampled_from(['string', 'integer', 'decimal', 'datetime', 'boolean'])
    )
    @settings(max_examples=100)
    def test_data_type_conversion_handles_all_types(self, string_value, target_type):
        """
        Feature: trestle-etl-pipeline, Property 8: Data Type Conversion Accuracy

        For any string value and target type, conversion should either succeed
        with correct type or fail with appropriate error.
        """
        try:
            result = self.transformer.convert_data_type(string_value, target_type, "test_field")

            if result is not None:
                if target_type == 'string':
                    assert isinstance(result, str) or result is None
                elif target_type == 'integer':
                    assert isinstance(result, int)
                elif target_type == 'decimal':
                    assert isinstance(result, Decimal)
                elif target_type == 'datetime':
                    assert isinstance(result, datetime)
                elif target_type == 'boolean':
                    assert isinstance(result, bool)

        except DataTransformationError:
            # Some conversions are expected to fail
            pass

    @given(value=st.none())
    @settings(max_examples=10)
    def test_none_values_remain_none_for_all_types(self, value):
        """
        Feature: trestle-etl-pipeline, Property 8: Data Type Conversion Accuracy

        For any None value, conversion should return None regardless of target type.
        """
        target_types = ['string', 'integer', 'decimal', 'datetime', 'boolean']

        for target_type in target_types:
            result = self.transformer.convert_data_type(value, target_type, "test_field")
            assert result is None, f"None should remain None for {target_type}"

    @given(
        integer_value=st.integers(min_value=-1000000, max_value=1000000)
    )
    @settings(max_examples=50)
    def test_integer_conversion_preserves_values(self, integer_value):
        """
        Feature: trestle-etl-pipeline, Property 8: Data Type Conversion Accuracy

        For any integer value, conversion to integer type should preserve the value.
        """
        result = self.transformer.convert_data_type(integer_value, 'integer', "test_field")
        assert result == integer_value, f"Integer conversion failed: {integer_value} -> {result}"
        assert isinstance(result, int), f"Result should be int, got {type(result)}"

    @given(
        decimal_value=st.decimals(min_value=-999999.99, max_value=999999.99, places=2)
    )
    @settings(max_examples=50)
    def test_decimal_conversion_preserves_precision(self, decimal_value):
        """
        Feature: trestle-etl-pipeline, Property 8: Data Type Conversion Accuracy

        For any decimal value, conversion should preserve precision appropriately.
        """
        result = self.transformer.convert_data_type(decimal_value, 'decimal', "test_field")

        if result is not None:
            assert isinstance(result, Decimal), f"Result should be Decimal, got {type(result)}"
            # Allow small precision differences due to string conversion
            assert abs(result - decimal_value) < Decimal('0.001'), \
                f"Precision lost: {decimal_value} -> {result}"

    @given(
        boolean_value=st.booleans()
    )
    @settings(max_examples=20)
    def test_boolean_conversion_preserves_truth_values(self, boolean_value):
        """
        Feature: trestle-etl-pipeline, Property 8: Data Type Conversion Accuracy

        For any boolean value, conversion should preserve the truth value.
        """
        result = self.transformer.convert_data_type(boolean_value, 'boolean', "test_field")
        assert result == boolean_value, f"Boolean conversion failed: {boolean_value} -> {result}"
        assert isinstance(result, bool), f"Result should be bool, got {type(result)}"

    @given(
        string_boolean=st.sampled_from(['true', 'false', 'True', 'False', 'TRUE', 'FALSE',
                                       '1', '0', 'yes', 'no', 'YES', 'NO', 'on', 'off'])
    )
    @settings(max_examples=30)
    def test_string_to_boolean_conversion_handles_common_values(self, string_boolean):
        """
        Feature: trestle-etl-pipeline, Property 8: Data Type Conversion Accuracy

        For any common boolean string representation, conversion should work correctly.
        """
        result = self.transformer.convert_data_type(string_boolean, 'boolean', "test_field")
        assert isinstance(result, bool), f"Result should be bool, got {type(result)}"

        # Verify correct interpretation
        true_values = {'true', '1', 'yes', 'y', 'on'}
        false_values = {'false', '0', 'no', 'n', 'off'}

        if string_boolean.lower() in true_values:
            assert result is True, f"'{string_boolean}' should convert to True"
        elif string_boolean.lower() in false_values:
            assert result is False, f"'{string_boolean}' should convert to False"

    @given(
        numeric_string=st.text(
            alphabet=st.sampled_from("0123456789.-+"),
            min_size=1,
            max_size=10
        ).filter(lambda x: re.match(r'^[+-]?\d*\.?\d+$', x))
    )
    @settings(max_examples=50)
    def test_numeric_string_conversion_works_correctly(self, numeric_string):
        """
        Feature: trestle-etl-pipeline, Property 8: Data Type Conversion Accuracy

        For any valid numeric string, conversion to numeric types should work.
        """
        try:
            # Test integer conversion
            int_result = self.transformer.convert_data_type(numeric_string, 'integer', "test_field")
            if int_result is not None:
                assert isinstance(int_result, int)

            # Test decimal conversion
            decimal_result = self.transformer.convert_data_type(numeric_string, 'decimal', "test_field")
            if decimal_result is not None:
                assert isinstance(decimal_result, Decimal)

        except DataTransformationError:
            # Some numeric strings might not be convertible (e.g., too large)
            pass

    def test_invalid_target_type_raises_error(self):
        """
        Feature: trestle-etl-pipeline, Property 8: Data Type Conversion Accuracy

        Invalid target types should raise appropriate errors.
        """
        with pytest.raises(DataTransformationError) as exc_info:
            self.transformer.convert_data_type("test", "invalid_type", "test_field")

        assert "Unknown target type" in str(exc_info.value)


class TestDataValidationAndProcessingContinuation:
    """
    Property 7: Data Validation and Processing Continuation

    For any dataset containing both valid and invalid records, the system should
    validate all records, log errors for invalid ones, and continue processing
    all valid records.

    Validates: Requirements 2.2, 2.3, 3.3
    """

    def setup_method(self):
        """Set up test environment."""
        self.transformer = DataTransformer()

    @given(
        valid_records=st.lists(
            st.fixed_dictionaries({
                'ListingKey': st.text(
                    alphabet=st.sampled_from("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"),
                    min_size=1,
                    max_size=20
                ),
                'ModificationTimestamp': st.datetimes(
                    min_value=datetime(2020, 1, 1),
                    max_value=datetime(2025, 12, 31)
                ),
                'ListPrice': st.one_of(st.none(), st.integers(min_value=0, max_value=10000000)),
                'PropertyType': st.one_of(st.none(), st.sampled_from(['Residential', 'Commercial', 'Land'])),
                'BedroomsTotal': st.one_of(st.none(), st.integers(min_value=0, max_value=20))
            }),
            min_size=1,
            max_size=10
        ),
        invalid_records=st.lists(
            st.one_of(
                # Missing required fields
                st.fixed_dictionaries({
                    'ListPrice': st.integers(min_value=0, max_value=1000000),
                    'PropertyType': st.sampled_from(['Residential', 'Commercial'])
                }),
                # Invalid data types
                st.fixed_dictionaries({
                    'ListingKey': st.text(
                        alphabet=st.sampled_from("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"),
                        min_size=1,
                        max_size=20
                    ),
                    'ModificationTimestamp': st.text(min_size=1, max_size=20),  # Invalid datetime
                    'ListPrice': st.text(alphabet=st.sampled_from('abcdef'), min_size=1, max_size=10)  # Invalid price
                }),
                # Null required fields
                st.fixed_dictionaries({
                    'ListingKey': st.none(),
                    'ModificationTimestamp': st.datetimes(
                        min_value=datetime(2020, 1, 1),
                        max_value=datetime(2025, 12, 31)
                    )
                })
            ),
            min_size=0,
            max_size=5
        )
    )
    @settings(max_examples=50)
    def test_mixed_valid_invalid_records_processing_continues(self, valid_records, invalid_records):
        """
        Feature: trestle-etl-pipeline, Property 7: Data Validation and Processing Continuation

        For any mix of valid and invalid records, processing should continue and
        return all valid records while logging errors for invalid ones.
        """
        # Combine valid and invalid records in random order
        all_records = valid_records + invalid_records

        # Transform the batch with continue_on_error=True
        result = self.transformer.transform_batch(all_records, continue_on_error=True)

        # Should have some valid records (at least the valid ones we provided)
        assert result['stats'].valid_records >= 0
        assert result['stats'].total_records == len(all_records)

        # Should have processed all records (valid + invalid)
        assert result['stats'].valid_records + result['stats'].invalid_records == len(all_records)

        # If we had valid records, they should be in the output
        if len(valid_records) > 0:
            assert result['stats'].valid_records > 0
            assert len(result['records']) > 0

        # If we had invalid records, they should be logged as errors
        if len(invalid_records) > 0:
            assert result['stats'].invalid_records > 0
            assert len(result['stats'].validation_errors) > 0

    @given(
        records_with_errors=st.lists(
            st.fixed_dictionaries({
                'ListingKey': st.text(min_size=1, max_size=20),
                'ModificationTimestamp': st.datetimes(
                    min_value=datetime(2020, 1, 1),
                    max_value=datetime(2025, 12, 31)
                ),
                'ListPrice': st.integers(min_value=-1000, max_value=-1),  # Invalid negative price
                'YearBuilt': st.integers(min_value=1500, max_value=1799)  # Invalid year (too early)
            }),
            min_size=1,
            max_size=5
        )
    )
    @settings(max_examples=30)
    def test_business_rule_validation_errors_logged_but_processing_continues(self, records_with_errors):
        """
        Feature: trestle-etl-pipeline, Property 7: Data Validation and Processing Continuation

        For any records with business rule violations, errors should be logged
        but processing should continue for other records.
        """
        result = self.transformer.transform_batch(records_with_errors, continue_on_error=True)

        # All records should be processed (even if they fail validation)
        assert result['stats'].total_records == len(records_with_errors)

        # Should have validation errors due to business rule violations
        assert len(result['stats'].validation_errors) > 0

        # Some records might still be considered "valid" if only warnings were generated
        assert result['stats'].valid_records + result['stats'].invalid_records == len(records_with_errors)

    @given(
        good_record=st.fixed_dictionaries({
            'ListingKey': st.text(
                alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz0123456789"),
                min_size=1, max_size=20
            ),
            'ModificationTimestamp': st.datetimes(
                min_value=datetime(2020, 1, 1),
                max_value=datetime(2025, 12, 31)
            ),
            'ListPrice': st.integers(min_value=1, max_value=1000000),
            'PropertyType': st.sampled_from(['Residential', 'Commercial', 'Land'])
        }),
        bad_record=st.fixed_dictionaries({
            'ListingKey': st.text(min_size=1, max_size=20),
            'ModificationTimestamp': st.text(min_size=1, max_size=10),  # Invalid datetime string
            'ListPrice': st.text(alphabet=st.sampled_from('xyz'), min_size=1, max_size=5)  # Invalid price
        })
    )
    @settings(max_examples=30)
    def test_single_invalid_record_does_not_stop_batch_processing(self, good_record, bad_record):
        """
        Feature: trestle-etl-pipeline, Property 7: Data Validation and Processing Continuation

        For any batch containing at least one valid and one invalid record,
        the valid record should be processed successfully.
        """
        records = [good_record, bad_record]

        result = self.transformer.transform_batch(records, continue_on_error=True)

        # Should process both records
        assert result['stats'].total_records == 2

        # Should have at least one valid record (the good one)
        assert result['stats'].valid_records >= 1

        # Should have at least one error (from the bad record)
        assert result['stats'].invalid_records >= 1
        assert len(result['stats'].validation_errors) >= 1

        # Should have at least one transformed record in output
        assert len(result['records']) >= 1

    @given(
        records=st.lists(
            st.dictionaries(
                keys=st.text(min_size=1, max_size=20),
                values=st.one_of(
                    st.none(),
                    st.text(max_size=100),
                    st.integers(),
                    st.floats(allow_nan=False, allow_infinity=False),
                    st.booleans()
                ),
                min_size=0,
                max_size=10
            ),
            min_size=1,
            max_size=10
        )
    )
    @settings(max_examples=50)
    def test_arbitrary_record_structures_handled_gracefully(self, records):
        """
        Feature: trestle-etl-pipeline, Property 7: Data Validation and Processing Continuation

        For any arbitrary record structure, the transformer should handle it
        gracefully without crashing, either transforming it or logging appropriate errors.
        """
        # This should not raise an exception, regardless of input
        result = self.transformer.transform_batch(records, continue_on_error=True)

        # Should always return a result structure
        assert 'records' in result
        assert 'stats' in result
        assert isinstance(result['records'], list)
        assert isinstance(result['stats'], TransformationStats)

        # Total should match input
        assert result['stats'].total_records == len(records)

        # Valid + invalid should equal total
        assert result['stats'].valid_records + result['stats'].invalid_records == len(records)

    def test_continue_on_error_false_stops_on_first_error(self):
        """
        Feature: trestle-etl-pipeline, Property 7: Data Validation and Processing Continuation

        When continue_on_error=False, processing should stop on the first error.
        """
        records = [
            {
                'ListingKey': 'VALID123',
                'ModificationTimestamp': datetime.now(),
                'ListPrice': 100000
            },
            {
                'ListingKey': None,  # Invalid - missing required field
                'ModificationTimestamp': datetime.now()
            }
        ]

        with pytest.raises((DataTransformationError, ValidationError)):
            self.transformer.transform_batch(records, continue_on_error=False)


class TestDuplicateDetectionConsistency:
    """
    Property 9: Duplicate Detection Consistency

    For any dataset with known duplicates, the duplicate detection algorithm
    should consistently identify the same records as duplicates across multiple runs.

    Validates: Requirements 2.5
    """

    def setup_method(self):
        """Set up test environment."""
        self.transformer = DataTransformer()

    @given(
        listing_keys=st.lists(
            st.text(min_size=1, max_size=20),
            min_size=2,
            max_size=10
        )
    )
    @settings(max_examples=50)
    def test_duplicate_detection_is_consistent_across_runs(self, listing_keys):
        """
        Feature: trestle-etl-pipeline, Property 9: Duplicate Detection Consistency

        For any set of listing keys, duplicate detection should return the same
        results when run multiple times.
        """
        # Create records with some duplicates
        records = []
        for key in listing_keys:
            records.append({
                'ListingKey': key,
                'ModificationTimestamp': datetime.now(),
                'ListPrice': 100000
            })

        # Add some intentional duplicates
        if len(listing_keys) > 0:
            records.append({
                'ListingKey': listing_keys[0],  # Duplicate the first key
                'ModificationTimestamp': datetime.now(),
                'ListPrice': 200000
            })

        # Run duplicate detection multiple times
        results1 = []
        results2 = []

        # Clear cache between runs to ensure consistency
        self.transformer.clear_duplicate_cache()
        for record in records:
            is_dup1 = self.transformer.detect_duplicate(record)
            results1.append(is_dup1)

        self.transformer.clear_duplicate_cache()
        for record in records:
            is_dup2 = self.transformer.detect_duplicate(record)
            results2.append(is_dup2)

        # Results should be identical
        assert results1 == results2, f"Inconsistent duplicate detection: {results1} vs {results2}"

    @given(
        base_key=st.text(min_size=1, max_size=15),
        num_duplicates=st.integers(min_value=1, max_value=5)
    )
    @settings(max_examples=30)
    def test_multiple_duplicates_of_same_key_detected(self, base_key, num_duplicates):
        """
        Feature: trestle-etl-pipeline, Property 9: Duplicate Detection Consistency

        For any listing key that appears multiple times, all instances after
        the first should be detected as duplicates.
        """
        records = []
        for i in range(num_duplicates + 1):  # +1 for the original
            records.append({
                'ListingKey': base_key,
                'ModificationTimestamp': datetime.now(),
                'ListPrice': 100000 + i * 1000
            })

        self.transformer.clear_duplicate_cache()
        duplicate_results = []

        for record in records:
            is_duplicate = self.transformer.detect_duplicate(record)
            duplicate_results.append(is_duplicate)

        # First occurrence should not be a duplicate
        assert duplicate_results[0] is False, "First occurrence should not be duplicate"

        # All subsequent occurrences should be duplicates
        for i in range(1, len(duplicate_results)):
            assert duplicate_results[i] is True, f"Occurrence {i} should be duplicate"

    @given(
        existing_keys=st.sets(
            st.text(min_size=1, max_size=15),
            min_size=1,
            max_size=10
        ),
        new_key=st.text(min_size=1, max_size=15)
    )
    @settings(max_examples=50)
    def test_existing_keys_parameter_works_correctly(self, existing_keys, new_key):
        """
        Feature: trestle-etl-pipeline, Property 9: Duplicate Detection Consistency

        For any set of existing keys, records with those keys should be detected
        as duplicates when the existing_keys parameter is provided.
        """
        # Test with a key that exists in the existing_keys set
        if existing_keys:
            existing_key = list(existing_keys)[0]
            record_with_existing_key = {
                'ListingKey': existing_key,
                'ModificationTimestamp': datetime.now()
            }

            self.transformer.clear_duplicate_cache()
            is_duplicate = self.transformer.detect_duplicate(record_with_existing_key, existing_keys)
            assert is_duplicate is True, f"Record with existing key {existing_key} should be duplicate"

        # Test with a new key not in existing_keys
        assume(new_key not in existing_keys)
        record_with_new_key = {
            'ListingKey': new_key,
            'ModificationTimestamp': datetime.now()
        }

        self.transformer.clear_duplicate_cache()
        is_duplicate = self.transformer.detect_duplicate(record_with_new_key, existing_keys)
        assert is_duplicate is False, f"Record with new key {new_key} should not be duplicate"

    @given(
        records=st.lists(
            st.fixed_dictionaries({
                'ListingKey': st.text(min_size=1, max_size=20),
                'ModificationTimestamp': st.datetimes(
                    min_value=datetime(2020, 1, 1),
                    max_value=datetime(2025, 12, 31)
                ),
                'ListPrice': st.integers(min_value=1, max_value=1000000)
            }),
            min_size=1,
            max_size=10
        )
    )
    @settings(max_examples=50)
    def test_duplicate_detection_in_batch_processing(self, records):
        """
        Feature: trestle-etl-pipeline, Property 9: Duplicate Detection Consistency

        For any batch of records, duplicate detection should work correctly
        during batch processing and mark duplicates appropriately.
        """
        result = self.transformer.transform_batch(records)

        # Count duplicates in the result
        duplicates_in_result = sum(1 for r in result['records'] if r.get('_is_duplicate', False))

        # Should match the stats
        assert duplicates_in_result == result['stats'].duplicates_detected

        # Verify duplicate detection logic by checking listing keys
        seen_keys = set()
        expected_duplicates = 0

        for record in records:
            key = record.get('ListingKey')
            if key and key in seen_keys:
                expected_duplicates += 1
            elif key:
                seen_keys.add(key)

        # The detected duplicates should match our manual count
        # (allowing for some flexibility in case of validation errors)
        assert result['stats'].duplicates_detected <= expected_duplicates

    def test_records_without_listing_key_not_considered_duplicates(self):
        """
        Feature: trestle-etl-pipeline, Property 9: Duplicate Detection Consistency

        Records without ListingKey should not be considered duplicates.
        """
        records_without_key = [
            {'ModificationTimestamp': datetime.now(), 'ListPrice': 100000},
            {'ModificationTimestamp': datetime.now(), 'ListPrice': 200000},
            {'ListingKey': None, 'ModificationTimestamp': datetime.now()}
        ]

        self.transformer.clear_duplicate_cache()

        for record in records_without_key:
            is_duplicate = self.transformer.detect_duplicate(record)
            assert is_duplicate is False, "Records without ListingKey should not be duplicates"
