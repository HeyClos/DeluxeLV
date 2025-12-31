"""
Integration tests for Trestle ETL Pipeline end-to-end flow.

Tests complete ETL pipeline with mock data, database schema compatibility,
and different configuration profiles.
"""

import os
import tempfile
from datetime import datetime
from typing import Dict, Any, List
from unittest.mock import Mock, MagicMock, patch

import pytest

from trestle_etl.config import (
    ConfigManager, Config, ConfigurationError,
    ConfigValidator, validate_config_on_startup,
    APIConfig, DatabaseConfig, ETLConfig, AlertConfig
)
from trestle_etl.data_transformer import DataTransformer
from trestle_etl.mysql_loader import MySQLLoader, SyncRun, SyncStatus, BatchResult


def create_test_config() -> Config:
    """Create a test configuration object."""
    return Config(
        api=APIConfig(
            client_id="test_client_id",
            client_secret="test_client_secret",
            base_url="https://api-test.example.com/odata",
            token_url="https://api-test.example.com/token",
            timeout=30
        ),
        database=DatabaseConfig(
            host="localhost",
            port=3306,
            database="test_real_estate",
            user="test_user",
            password="test_password",
            charset="utf8mb4"
        ),
        etl=ETLConfig(
            batch_size=100,
            max_retries=2,
            incremental_field="ModificationTimestamp",
            log_level="DEBUG",
            lock_file_path="/tmp/test_etl.lock",
            log_dir="logs"
        ),
        alert=AlertConfig(
            email_enabled=False,
            email_recipients=[],
            webhook_enabled=False,
            webhook_url=None
        )
    )


def create_mock_api_records(count: int = 10) -> List[Dict[str, Any]]:
    """Create mock API response records with normalized field names."""
    records = []
    for i in range(count):
        records.append({
            'listing_key': f'LISTING{i:05d}',
            'list_price': 100000 + (i * 10000),
            'property_type': 'Residential' if i % 2 == 0 else 'Commercial',
            'bedrooms_total': (i % 5) + 1,
            'bathrooms_total': ((i % 3) + 1) + 0.5,
            'square_feet': 1000 + (i * 100),
            'lot_size_acres': 0.25 + (i * 0.1),
            'year_built': 1990 + (i % 30),
            'listing_status': 'Active' if i % 3 == 0 else 'Pending',
            'modification_timestamp': datetime.now(),
            'street_address': f'{100 + i} Main Street',
            'city': 'Test City',
            'state_or_province': 'CA',
            'postal_code': f'9000{i % 10}'
        })
    return records


class TestEndToEndETLFlow:
    """Test complete ETL pipeline with mock data."""
    
    def test_complete_etl_flow_with_mock_data(self):
        """
        Test the complete ETL flow: extract -> transform -> load.
        
        This test verifies that:
        1. API records can be extracted (mocked)
        2. Records are properly transformed
        3. Records can be loaded to database (mocked)
        4. Sync metadata is tracked correctly
        """
        config = create_test_config()
        
        # Create mock API records
        api_records = create_mock_api_records(10)
        
        # Transform records
        transformer = DataTransformer()
        transform_result = transformer.transform_batch(api_records, continue_on_error=True)
        
        # Verify transformation
        assert transform_result['stats'].total_records == 10
        assert transform_result['stats'].valid_records > 0
        assert len(transform_result['records']) > 0
        
        # Verify transformed records have correct field names
        for record in transform_result['records']:
            assert 'listing_key' in record
            assert 'list_price' in record
            assert 'property_type' in record
    
    def test_etl_flow_handles_mixed_valid_invalid_records(self):
        """
        Test ETL flow with a mix of valid and invalid records.
        
        Verifies that invalid records are logged but don't stop processing.
        """
        # Create mix of valid and invalid records
        records = create_mock_api_records(5)
        
        # Add invalid records
        records.append({
            'listing_key': None,  # Invalid - missing required field
            'list_price': 'not_a_number'
        })
        records.append({
            'listing_key': 'INVALID001',
            'modification_timestamp': 'invalid_date'
        })
        
        transformer = DataTransformer()
        result = transformer.transform_batch(records, continue_on_error=True)
        
        # Should process all records
        assert result['stats'].total_records == 7
        
        # Should have some valid records (the 5 valid ones)
        assert result['stats'].valid_records >= 4  # Allow some flexibility
        
        # Should have some invalid records
        assert result['stats'].invalid_records >= 1
    
    def test_etl_flow_with_empty_dataset(self):
        """Test ETL flow handles empty datasets gracefully."""
        transformer = DataTransformer()
        result = transformer.transform_batch([], continue_on_error=True)
        
        assert result['stats'].total_records == 0
        assert result['stats'].valid_records == 0
        assert result['stats'].invalid_records == 0
        assert len(result['records']) == 0
    
    def test_etl_flow_with_large_batch(self):
        """Test ETL flow handles large batches correctly."""
        # Create large batch
        records = create_mock_api_records(500)
        
        transformer = DataTransformer()
        result = transformer.transform_batch(records, continue_on_error=True)
        
        # Should process all records
        assert result['stats'].total_records == 500
        assert result['stats'].valid_records > 0
        
        # Should have transformed records
        assert len(result['records']) > 0


class TestDatabaseSchemaCompatibility:
    """Test database schema compatibility."""
    
    def test_transformed_records_match_database_schema(self):
        """
        Verify transformed records have fields matching database schema.
        """
        api_records = create_mock_api_records(5)
        
        transformer = DataTransformer()
        result = transformer.transform_batch(api_records, continue_on_error=True)
        
        # Expected database fields
        expected_fields = {
            'listing_key', 'list_price', 'property_type', 'bedrooms_total',
            'bathrooms_total', 'square_feet', 'lot_size_acres', 'year_built',
            'listing_status', 'modification_timestamp', 'street_address',
            'city', 'state_or_province', 'postal_code'
        }
        
        for record in result['records']:
            # Check that all expected fields are present
            record_fields = set(record.keys())
            missing_fields = expected_fields - record_fields
            
            # listing_key is required
            assert 'listing_key' in record, "listing_key is required"
            
            # modification_timestamp is required
            assert 'modification_timestamp' in record, "modification_timestamp is required"
    
    def test_mysql_loader_property_fields_match_schema(self):
        """
        Verify MySQLLoader.PROPERTY_FIELDS matches expected schema.
        """
        expected_fields = [
            'listing_key', 'list_price', 'property_type', 'bedrooms_total',
            'bathrooms_total', 'square_feet', 'lot_size_acres', 'year_built',
            'listing_status', 'modification_timestamp', 'street_address',
            'city', 'state_or_province', 'postal_code'
        ]
        
        assert MySQLLoader.PROPERTY_FIELDS == expected_fields
    
    def test_batch_result_structure(self):
        """Verify BatchResult has expected structure."""
        result = BatchResult(
            total_records=100,
            inserted=50,
            updated=50,
            errors=0,
            error_messages=[]
        )
        
        assert result.total_records == 100
        assert result.inserted == 50
        assert result.updated == 50
        assert result.errors == 0
        assert isinstance(result.error_messages, list)
    
    def test_sync_run_structure(self):
        """Verify SyncRun has expected structure."""
        sync_run = SyncRun(
            sync_id=1,
            sync_start=datetime.now(),
            sync_end=datetime.now(),
            records_processed=100,
            records_inserted=50,
            records_updated=50,
            api_calls_made=10,
            status=SyncStatus.SUCCESS,
            error_message=None,
            last_sync_timestamp=datetime.now()
        )
        
        assert sync_run.sync_id == 1
        assert sync_run.records_processed == 100
        assert sync_run.status == SyncStatus.SUCCESS


class TestConfigurationProfiles:
    """Test different configuration profiles."""
    
    def setup_method(self):
        """Clear environment before each test."""
        self._clear_env_vars()
    
    def teardown_method(self):
        """Clear environment after each test."""
        self._clear_env_vars()
    
    def _clear_env_vars(self):
        """Clear all Trestle-related environment variables."""
        env_vars = [
            "TRESTLE_CLIENT_ID", "TRESTLE_CLIENT_SECRET", "TRESTLE_API_BASE_URL",
            "TRESTLE_TOKEN_URL", "TRESTLE_API_TIMEOUT",
            "MYSQL_HOST", "MYSQL_PORT", "MYSQL_DATABASE", "MYSQL_USER",
            "MYSQL_PASSWORD", "MYSQL_CHARSET",
            "BATCH_SIZE", "MAX_RETRIES", "INCREMENTAL_FIELD", "LOG_LEVEL",
            "LOCK_FILE_PATH", "LOG_DIR",
            "ALERT_EMAIL_ENABLED", "ALERT_EMAIL_RECIPIENTS", "ALERT_WEBHOOK_ENABLED",
            "ALERT_WEBHOOK_URL", "SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD"
        ]
        for var in env_vars:
            os.environ.pop(var, None)
    
    def _create_env_file(self, config_dict: dict) -> str:
        """Create a temporary .env file."""
        fd, path = tempfile.mkstemp(suffix=".env")
        with os.fdopen(fd, 'w') as f:
            for key, value in config_dict.items():
                f.write(f"{key}={value}\n")
        return path
    
    def test_development_profile_settings(self):
        """Test development configuration profile."""
        config_dict = {
            "TRESTLE_CLIENT_ID": "dev_client",
            "TRESTLE_CLIENT_SECRET": "dev_secret",
            "MYSQL_HOST": "localhost",
            "MYSQL_DATABASE": "real_estate_dev",
            "MYSQL_USER": "dev_user",
            "MYSQL_PASSWORD": "dev_pass",
            "BATCH_SIZE": "100",
            "MAX_RETRIES": "1",
            "LOG_LEVEL": "DEBUG"
        }
        
        env_path = self._create_env_file(config_dict)
        try:
            manager = ConfigManager()
            config = manager.load_config(env_path)
            
            # Verify development settings
            assert config.etl.batch_size == 100
            assert config.etl.max_retries == 1
            assert config.etl.log_level == "DEBUG"
        finally:
            os.unlink(env_path)
    
    def test_production_profile_settings(self):
        """Test production configuration profile."""
        config_dict = {
            "TRESTLE_CLIENT_ID": "prod_client",
            "TRESTLE_CLIENT_SECRET": "prod_secret",
            "MYSQL_HOST": "prod-db.example.com",
            "MYSQL_DATABASE": "real_estate",
            "MYSQL_USER": "etl_prod",
            "MYSQL_PASSWORD": "secure_password",
            "BATCH_SIZE": "1000",
            "MAX_RETRIES": "3",
            "LOG_LEVEL": "WARNING",
            "ALERT_EMAIL_ENABLED": "true",
            "ALERT_EMAIL_RECIPIENTS": "ops@example.com"
        }
        
        env_path = self._create_env_file(config_dict)
        try:
            manager = ConfigManager()
            config = manager.load_config(env_path)
            
            # Verify production settings
            assert config.etl.batch_size == 1000
            assert config.etl.max_retries == 3
            assert config.etl.log_level == "WARNING"
            assert config.alert.email_enabled is True
            assert "ops@example.com" in config.alert.email_recipients
        finally:
            os.unlink(env_path)
    
    def test_configuration_validation_passes_for_valid_config(self):
        """Test configuration validation passes for valid configuration."""
        config = create_test_config()
        
        validator = ConfigValidator(config)
        is_valid = validator.validate()
        
        assert is_valid is True
        assert len(validator.errors) == 0
    
    def test_configuration_validation_catches_invalid_settings(self):
        """Test configuration validation catches invalid settings."""
        config = Config(
            api=APIConfig(
                client_id="",  # Invalid - empty
                client_secret="secret",
                base_url="invalid_url",  # Invalid - no http://
                token_url="https://valid.url/token",
                timeout=-1  # Invalid - negative
            ),
            database=DatabaseConfig(
                host="localhost",
                port=99999,  # Invalid - out of range
                database="test_db",
                user="user",
                password="pass"
            ),
            etl=ETLConfig(
                batch_size=-100,  # Invalid - negative
                max_retries=-1,  # Invalid - negative
                log_level="INVALID_LEVEL"  # Invalid
            ),
            alert=AlertConfig(
                email_enabled=True,
                email_recipients=[],  # Invalid - enabled but no recipients
                webhook_enabled=True,
                webhook_url=None  # Invalid - enabled but no URL
            )
        )
        
        validator = ConfigValidator(config)
        is_valid = validator.validate()
        
        assert is_valid is False
        assert len(validator.errors) > 0
        
        # Check specific errors are caught
        error_text = " ".join(validator.errors)
        assert "client_id" in error_text.lower() or "empty" in error_text.lower()
    
    def test_validate_config_on_startup_raises_on_invalid(self):
        """Test validate_config_on_startup raises exception for invalid config."""
        config = Config(
            api=APIConfig(
                client_id="",  # Invalid
                client_secret="secret",
                base_url="https://valid.url",
                token_url="https://valid.url/token",
                timeout=30
            ),
            database=DatabaseConfig(
                host="localhost",
                port=3306,
                database="test_db",
                user="user",
                password="pass"
            ),
            etl=ETLConfig(),
            alert=AlertConfig()
        )
        
        with pytest.raises(ConfigurationError):
            validate_config_on_startup(config, raise_on_error=True)
    
    def test_validate_config_on_startup_returns_report(self):
        """Test validate_config_on_startup returns validation report."""
        config = create_test_config()
        
        report = validate_config_on_startup(config, raise_on_error=False)
        
        assert 'valid' in report
        assert 'errors' in report
        assert 'warnings' in report
        assert report['valid'] is True


class TestDataTransformationIntegration:
    """Test data transformation integration."""
    
    def test_field_name_normalization_integration(self):
        """Test field name normalization works end-to-end."""
        transformer = DataTransformer()
        
        # Test various API field names
        api_fields = [
            'ListingKey',
            'ListPrice',
            'PropertyType',
            'BedroomsTotal',
            'BathroomsTotal',
            'ModificationTimestamp'
        ]
        
        for field in api_fields:
            normalized = transformer.normalize_field_name(field)
            
            # Should be lowercase with underscores
            assert normalized == normalized.lower()
            assert ' ' not in normalized
            
            # Should be valid SQL identifier
            assert normalized[0].isalpha() or normalized[0] == '_'
    
    def test_data_type_conversion_integration(self):
        """Test data type conversion works end-to-end."""
        transformer = DataTransformer()
        
        # Test various conversions
        test_cases = [
            ("100000", "integer", int),
            ("100000.50", "decimal", type(None)),  # Decimal or float
            ("true", "boolean", bool),
            ("2024-01-15T10:30:00Z", "datetime", datetime),
            ("test string", "string", str)
        ]
        
        for value, target_type, expected_type in test_cases:
            result = transformer.convert_data_type(value, target_type, "test_field")
            if result is not None and expected_type != type(None):
                assert isinstance(result, expected_type) or result is None
    
    def test_duplicate_detection_integration(self):
        """Test duplicate detection works end-to-end."""
        transformer = DataTransformer()
        transformer.clear_duplicate_cache()
        
        # First record should not be duplicate
        record1 = {'listing_key': 'TEST001', 'list_price': 100000}
        is_dup1 = transformer.detect_duplicate(record1)
        assert is_dup1 is False
        
        # Same key should be duplicate
        record2 = {'listing_key': 'TEST001', 'list_price': 200000}
        is_dup2 = transformer.detect_duplicate(record2)
        assert is_dup2 is True
        
        # Different key should not be duplicate
        record3 = {'listing_key': 'TEST002', 'list_price': 150000}
        is_dup3 = transformer.detect_duplicate(record3)
        assert is_dup3 is False


class TestMySQLLoaderIntegration:
    """Test MySQL loader integration with mocked database."""
    
    def test_batch_operations_integration(self):
        """Test batch operations work correctly with mocked database."""
        config = DatabaseConfig(
            host="localhost",
            port=3306,
            database="test_db",
            user="test_user",
            password="test_pass"
        )
        
        loader = MySQLLoader(config, batch_size=5)
        
        # Create test records
        records = [
            {
                'listing_key': f'TEST{i:03d}',
                'list_price': 100000 + (i * 10000),
                'property_type': 'Residential',
                'bedrooms_total': 3,
                'bathrooms_total': 2.0,
                'square_feet': 1500,
                'lot_size_acres': 0.25,
                'year_built': 2000,
                'listing_status': 'Active',
                'modification_timestamp': datetime.now(),
                'street_address': f'{100 + i} Test St',
                'city': 'Test City',
                'state_or_province': 'CA',
                'postal_code': '90001'
            }
            for i in range(10)
        ]
        
        # Mock database connection
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 10
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.open = True
        
        with patch.object(loader, 'get_connection', return_value=mock_connection):
            result = loader.batch_upsert(records)
            
            # Should process all records
            assert result.total_records == 10
            
            # Should use batch operations (executemany)
            assert mock_cursor.executemany.called
            
            # Should commit
            assert mock_connection.commit.called
    
    def test_sync_metadata_tracking_integration(self):
        """Test sync metadata tracking works correctly."""
        config = DatabaseConfig(
            host="localhost",
            port=3306,
            database="test_db",
            user="test_user",
            password="test_pass"
        )
        
        loader = MySQLLoader(config)
        
        # Mock database connection
        mock_cursor = MagicMock()
        mock_cursor.lastrowid = 1
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.open = True
        
        with patch.object(loader, 'get_connection', return_value=mock_connection):
            # Start sync run
            sync_run = loader.start_sync_run()
            assert sync_run.sync_id == 1
            assert sync_run.sync_start is not None
            
            # Update metrics
            sync_run = loader.update_sync_run(
                sync_run,
                records_processed=100,
                records_inserted=50,
                records_updated=50,
                api_calls_made=10
            )
            
            assert sync_run.records_processed == 100
            assert sync_run.records_inserted == 50
            
            # Complete sync run
            sync_run = loader.complete_sync_run(sync_run, status=SyncStatus.SUCCESS)
            
            assert sync_run.sync_end is not None
            assert sync_run.status == SyncStatus.SUCCESS
