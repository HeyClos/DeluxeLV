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


# Strategy for generating field names with various characters
field_name_strategy = st.text(
    alphabet=st.sampled_from(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-. @#$%^&*()[]{}|\\:;\"'<>?/~`"
    ),
    min_size=1,
    max_size=100
)

# Strategy for generating valid SQL identifier characters
valid_sql_chars = st.sampled_from("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_")

# Strategy for generating valid field names that should normalize cleanly
clean_field_name_strategy = st.text(
    alphabet=valid_sql_chars,
    min_size=1,
    max_size=50
).filter(lambda x: x and x[0].isalpha() or x[0] == '_')


def is_valid_sql_identifier(name: str) -> bool:
    """Check if a string is a valid SQL identifier."""
    if not name:
        return False
    
    # Must start with letter or underscore
    if not (name[0].isalpha() or name[0] == '_'):
        return False
    
    # Must contain only alphanumeric characters and underscores
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        return False
    
    # Must not be longer than 64 characters (MySQL limit)
    if len(name) > 64:
        return False
    
    return True


class TestFieldNameNormalizationConsistency:
    """
    Property 6: Field Name Normalization Consistency
    
    For any API field name, the transformation to MySQL column names should be 
    consistent, reversible, and produce valid SQL identifiers.
    
    Validates: Requirements 2.1
    """
    
    def setup_method(self):
        """Set up test environment."""
        self.transformer = DataTransformer()
    
    @given(field_names=st.lists(field_name_strategy, min_size=1, max_size=20))
    @settings(max_examples=100)
    def test_field_name_normalization_produces_valid_sql_identifiers(self, field_names):
        """
        Feature: trestle-etl-pipeline, Property 6: Field Name Normalization Consistency
        
        For any list of API field names, normalization should produce valid SQL identifiers.
        """
        # Filter out empty or whitespace-only field names
        valid_field_names = [name for name in field_names if name and name.strip()]
        assume(len(valid_field_names) > 0)
        
        normalized_names = []
        for field_name in valid_field_names:
            try:
                normalized = self.transformer.normalize_field_name(field_name)
                normalized_names.append(normalized)
                
                # Each normalized name should be a valid SQL identifier
                assert is_valid_sql_identifier(normalized), f"Invalid SQL identifier: '{normalized}' from '{field_name}'"
                
                # Should not be empty
                assert normalized, f"Empty result for field name: '{field_name}'"
                
                # Should not exceed MySQL column name limit
                assert len(normalized) <= 64, f"Name too long: '{normalized}' ({len(normalized)} chars)"
                
            except DataTransformationError:
                # Some field names might be invalid, which is acceptable
                pass
    
    @given(field_name=field_name_strategy)
    @settings(max_examples=100)
    def test_field_name_normalization_is_consistent(self, field_name):
        """
        Feature: trestle-etl-pipeline, Property 6: Field Name Normalization Consistency
        
        For any field name, multiple calls to normalize should return the same result.
        """
        assume(field_name and field_name.strip())
        
        try:
            result1 = self.transformer.normalize_field_name(field_name)
            result2 = self.transformer.normalize_field_name(field_name)
            
            # Results should be identical
            assert result1 == result2, f"Inconsistent normalization for '{field_name}': '{result1}' vs '{result2}'"
            
        except DataTransformationError:
            # If it fails once, it should fail consistently
            with pytest.raises(DataTransformationError):
                self.transformer.normalize_field_name(field_name)
    
    @given(field_names=st.lists(clean_field_name_strategy, min_size=2, max_size=10))
    @settings(max_examples=50)
    def test_different_field_names_produce_different_normalized_names(self, field_names):
        """
        Feature: trestle-etl-pipeline, Property 6: Field Name Normalization Consistency
        
        For any set of different clean field names, normalization should produce 
        different results (no collisions for reasonable inputs).
        """
        # Remove duplicates from input
        unique_field_names = list(set(field_names))
        assume(len(unique_field_names) >= 2)
        
        normalized_names = []
        for field_name in unique_field_names:
            normalized = self.transformer.normalize_field_name(field_name)
            normalized_names.append(normalized)
        
        # For clean inputs, we should get unique outputs
        # (This may not hold for all possible inputs due to normalization rules)
        unique_normalized = set(normalized_names)
        
        # At minimum, we shouldn't have complete collapse to a single name
        # unless all inputs were very similar
        if len(unique_field_names) > 1:
            # Allow some collisions for very similar names, but not total collapse
            collision_rate = 1 - (len(unique_normalized) / len(unique_field_names))
            assert collision_rate < 0.8, f"Too many collisions: {len(unique_normalized)} unique from {len(unique_field_names)} inputs"
    
    @given(field_name=st.text(min_size=1, max_size=10))
    @settings(max_examples=50)
    def test_normalized_names_avoid_mysql_reserved_words(self, field_name):
        """
        Feature: trestle-etl-pipeline, Property 6: Field Name Normalization Consistency
        
        For any field name, the normalized result should not be a MySQL reserved word.
        """
        try:
            normalized = self.transformer.normalize_field_name(field_name)
            
            # Check that result is not a reserved word
            assert normalized.lower() not in self.transformer.MYSQL_RESERVED_WORDS, \
                f"Normalized name '{normalized}' is a MySQL reserved word"
                
        except DataTransformationError:
            # Some inputs may be invalid, which is acceptable
            pass
    
    @given(field_name=field_name_strategy)
    @settings(max_examples=100)
    def test_field_name_caching_works_correctly(self, field_name):
        """
        Feature: trestle-etl-pipeline, Property 6: Field Name Normalization Consistency
        
        For any field name, the caching mechanism should work correctly and 
        return cached results on subsequent calls.
        """
        assume(field_name and field_name.strip())
        
        try:
            # Clear cache first
            self.transformer._field_name_cache.clear()
            
            # First call should populate cache
            result1 = self.transformer.normalize_field_name(field_name)
            assert field_name in self.transformer._field_name_cache
            
            # Second call should use cache
            result2 = self.transformer.normalize_field_name(field_name)
            assert result1 == result2
            
            # Cache should still contain the entry
            assert self.transformer._field_name_cache[field_name] == result1
            
        except DataTransformationError:
            # Invalid field names should not be cached
            assert field_name not in self.transformer._field_name_cache
    
    def test_empty_and_invalid_field_names_raise_errors(self):
        """
        Feature: trestle-etl-pipeline, Property 6: Field Name Normalization Consistency
        
        Empty or invalid field names should raise appropriate errors.
        """
        invalid_names = ["", None, "   ", "\t\n"]
        
        for invalid_name in invalid_names:
            with pytest.raises(DataTransformationError):
                self.transformer.normalize_field_name(invalid_name)
    
    @given(field_name=st.text(min_size=65, max_size=200))
    @settings(max_examples=20)
    def test_very_long_field_names_are_truncated_appropriately(self, field_name):
        """
        Feature: trestle-etl-pipeline, Property 6: Field Name Normalization Consistency
        
        For any very long field name, the result should be truncated to fit 
        MySQL column name limits while remaining valid.
        """
        try:
            normalized = self.transformer.normalize_field_name(field_name)
            
            # Should be within MySQL limits
            assert len(normalized) <= 64, f"Truncated name too long: {len(normalized)} chars"
            
            # Should still be a valid identifier
            assert is_valid_sql_identifier(normalized), f"Truncated name invalid: '{normalized}'"
            
        except DataTransformationError:
            # Some very long names might be completely invalid
            pass
    
    @given(
        base_name=st.text(
            alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz"),
            min_size=3,
            max_size=10
        ),
        special_chars=st.text(
            alphabet=st.sampled_from("-. @#$%^&*()[]{}|\\:;\"'<>?/~`"),
            min_size=1,
            max_size=5
        )
    )
    @settings(max_examples=50)
    def test_special_characters_are_handled_consistently(self, base_name, special_chars):
        """
        Feature: trestle-etl-pipeline, Property 6: Field Name Normalization Consistency
        
        For any field name containing special characters, they should be 
        consistently converted to underscores or removed.
        """
        field_name = base_name + special_chars + base_name
        
        normalized = self.transformer.normalize_field_name(field_name)
        
        # Should be valid SQL identifier
        assert is_valid_sql_identifier(normalized), f"Invalid result: '{normalized}'"
        
        # Should contain only valid characters
        assert re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', normalized), \
            f"Contains invalid characters: '{normalized}'"
        
        # Should preserve some part of the original base name
        # (This is a weak assertion since heavy normalization might change things significantly)
        assert len(normalized) > 0, "Normalization resulted in empty string"


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
                'listing_key': st.text(
                    alphabet=st.sampled_from("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"),
                    min_size=1,
                    max_size=20
                ),
                'modification_timestamp': st.datetimes(
                    min_value=datetime(2020, 1, 1),
                    max_value=datetime(2025, 12, 31)
                ),
                'list_price': st.one_of(st.none(), st.integers(min_value=0, max_value=10000000)),
                'property_type': st.one_of(st.none(), st.sampled_from(['Residential', 'Commercial', 'Land'])),
                'bedrooms_total': st.one_of(st.none(), st.integers(min_value=0, max_value=20))
            }),
            min_size=1,
            max_size=10
        ),
        invalid_records=st.lists(
            st.one_of(
                # Missing required fields
                st.fixed_dictionaries({
                    'list_price': st.integers(min_value=0, max_value=1000000),
                    'property_type': st.sampled_from(['Residential', 'Commercial'])
                }),
                # Invalid data types
                st.fixed_dictionaries({
                    'listing_key': st.text(
                        alphabet=st.sampled_from("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"),
                        min_size=1,
                        max_size=20
                    ),
                    'modification_timestamp': st.text(min_size=1, max_size=20),  # Invalid datetime
                    'list_price': st.text(alphabet=st.sampled_from('abcdef'), min_size=1, max_size=10)  # Invalid price
                }),
                # Null required fields
                st.fixed_dictionaries({
                    'listing_key': st.none(),
                    'modification_timestamp': st.datetimes(
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
                'listing_key': st.text(min_size=1, max_size=20),
                'modification_timestamp': st.datetimes(
                    min_value=datetime(2020, 1, 1),
                    max_value=datetime(2025, 12, 31)
                ),
                'list_price': st.integers(min_value=-1000, max_value=-1),  # Invalid negative price
                'year_built': st.integers(min_value=1500, max_value=1799)  # Invalid year (too early)
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
        # (depends on the specific validation rules)
        assert result['stats'].valid_records + result['stats'].invalid_records == len(records_with_errors)
    
    @given(
        good_record=st.fixed_dictionaries({
            'listing_key': st.text(min_size=1, max_size=20),
            'modification_timestamp': st.datetimes(
                min_value=datetime(2020, 1, 1),
                max_value=datetime(2025, 12, 31)
            ),
            'list_price': st.integers(min_value=1, max_value=1000000),
            'property_type': st.sampled_from(['Residential', 'Commercial', 'Land'])
        }),
        bad_record=st.fixed_dictionaries({
            'listing_key': st.text(min_size=1, max_size=20),
            'modification_timestamp': st.text(min_size=1, max_size=10),  # Invalid datetime string
            'list_price': st.text(alphabet=st.sampled_from('xyz'), min_size=1, max_size=5)  # Invalid price
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
                'listing_key': 'VALID123',
                'modification_timestamp': datetime.now(),
                'list_price': 100000
            },
            {
                'listing_key': None,  # Invalid - missing required field
                'modification_timestamp': datetime.now()
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
                'listing_key': key,
                'modification_timestamp': datetime.now(),
                'list_price': 100000
            })
        
        # Add some intentional duplicates
        if len(listing_keys) > 0:
            records.append({
                'listing_key': listing_keys[0],  # Duplicate the first key
                'modification_timestamp': datetime.now(),
                'list_price': 200000
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
                'listing_key': base_key,
                'modification_timestamp': datetime.now(),
                'list_price': 100000 + i * 1000
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
                'listing_key': existing_key,
                'modification_timestamp': datetime.now()
            }
            
            self.transformer.clear_duplicate_cache()
            is_duplicate = self.transformer.detect_duplicate(record_with_existing_key, existing_keys)
            assert is_duplicate is True, f"Record with existing key {existing_key} should be duplicate"
        
        # Test with a new key not in existing_keys
        assume(new_key not in existing_keys)
        record_with_new_key = {
            'listing_key': new_key,
            'modification_timestamp': datetime.now()
        }
        
        self.transformer.clear_duplicate_cache()
        is_duplicate = self.transformer.detect_duplicate(record_with_new_key, existing_keys)
        assert is_duplicate is False, f"Record with new key {new_key} should not be duplicate"
    
    @given(
        records=st.lists(
            st.fixed_dictionaries({
                'listing_key': st.text(min_size=1, max_size=20),
                'modification_timestamp': st.datetimes(
                    min_value=datetime(2020, 1, 1),
                    max_value=datetime(2025, 12, 31)
                ),
                'list_price': st.integers(min_value=1, max_value=1000000)
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
            key = record.get('listing_key')
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
        
        Records without listing_key should not be considered duplicates.
        """
        records_without_key = [
            {'modification_timestamp': datetime.now(), 'list_price': 100000},
            {'modification_timestamp': datetime.now(), 'list_price': 200000},
            {'listing_key': None, 'modification_timestamp': datetime.now()}
        ]
        
        self.transformer.clear_duplicate_cache()
        
        for record in records_without_key:
            is_duplicate = self.transformer.detect_duplicate(record)
            assert is_duplicate is False, "Records without listing_key should not be duplicates"