"""
Property-based tests for Configuration Manager.

Feature: trestle-etl-pipeline, Property 20: Configuration Loading Completeness
Validates: Requirements 7.1, 7.2, 7.3, 7.4
"""

import os
import tempfile
from pathlib import Path

import pytest
from hypothesis import given, strategies as st, settings, assume

from trestle_etl.config import ConfigManager, ConfigurationError, Config


# Strategy for generating valid environment variable names
valid_env_var_chars = st.sampled_from(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_"
)

# Strategy for generating non-empty strings suitable for config values
config_value_strategy = st.text(
    alphabet=st.sampled_from(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-./:"
    ),
    min_size=1,
    max_size=100
)

# Strategy for generating valid port numbers
port_strategy = st.integers(min_value=1, max_value=65535)

# Strategy for generating valid batch sizes
batch_size_strategy = st.integers(min_value=1, max_value=10000)


def create_env_file(config_dict: dict) -> str:
    """Create a temporary .env file with the given configuration."""
    fd, path = tempfile.mkstemp(suffix=".env")
    with os.fdopen(fd, 'w') as f:
        for key, value in config_dict.items():
            f.write(f"{key}={value}\n")
    return path


def cleanup_env_file(path: str) -> None:
    """Remove temporary env file."""
    try:
        os.unlink(path)
    except OSError:
        pass


TRESTLE_ENV_VARS = [
    "TRESTLE_CLIENT_ID", "TRESTLE_CLIENT_SECRET", "TRESTLE_API_BASE_URL",
    "TRESTLE_TOKEN_URL", "TRESTLE_API_TIMEOUT",
    "MYSQL_HOST", "MYSQL_PORT", "MYSQL_DATABASE", "MYSQL_USER", 
    "MYSQL_PASSWORD", "MYSQL_CHARSET",
    "BATCH_SIZE", "MAX_RETRIES", "INCREMENTAL_FIELD", "LOG_LEVEL",
    "LOCK_FILE_PATH", "LOG_DIR",
    "ALERT_EMAIL_ENABLED", "ALERT_EMAIL_RECIPIENTS", "ALERT_WEBHOOK_ENABLED",
    "ALERT_WEBHOOK_URL", "SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD"
]


def clear_env_vars():
    """Clear all Trestle-related environment variables."""
    for var in TRESTLE_ENV_VARS:
        os.environ.pop(var, None)


class TestConfigurationLoadingCompleteness:
    """
    Property 20: Configuration Loading Completeness
    
    For any valid configuration environment, all required settings should be 
    loaded correctly and invalid configurations should be rejected with clear 
    error messages.
    
    Validates: Requirements 7.1, 7.2, 7.3, 7.4
    """
    
    def setup_method(self):
        """Clear environment before each test."""
        clear_env_vars()
    
    def teardown_method(self):
        """Clear environment after each test."""
        clear_env_vars()

    @given(
        client_id=config_value_strategy,
        client_secret=config_value_strategy,
        mysql_host=config_value_strategy,
        mysql_database=config_value_strategy,
        mysql_user=config_value_strategy,
        mysql_password=config_value_strategy
    )
    @settings(max_examples=20)
    def test_valid_config_loads_all_required_settings(
        self, client_id, client_secret, mysql_host, mysql_database, mysql_user, mysql_password
    ):
        """
        Feature: trestle-etl-pipeline, Property 20: Configuration Loading Completeness
        
        For any valid set of required configuration values, the ConfigManager
        should successfully load all settings without error.
        """
        config_dict = {
            "TRESTLE_CLIENT_ID": client_id,
            "TRESTLE_CLIENT_SECRET": client_secret,
            "MYSQL_HOST": mysql_host,
            "MYSQL_DATABASE": mysql_database,
            "MYSQL_USER": mysql_user,
            "MYSQL_PASSWORD": mysql_password,
        }
        
        env_path = create_env_file(config_dict)
        try:
            clear_env_vars()  # Clear before loading
            manager = ConfigManager()
            config = manager.load_config(env_path)
            
            # Verify all required settings are loaded correctly
            assert config.api.client_id == client_id
            assert config.api.client_secret == client_secret
            assert config.database.host == mysql_host
            assert config.database.database == mysql_database
            assert config.database.user == mysql_user
            assert config.database.password == mysql_password
            
            # Verify config object is complete
            assert isinstance(config, Config)
            assert config.api is not None
            assert config.database is not None
            assert config.etl is not None
            assert config.alert is not None
        finally:
            cleanup_env_file(env_path)
            clear_env_vars()


    @given(
        client_id=config_value_strategy,
        client_secret=config_value_strategy,
        mysql_host=config_value_strategy,
        mysql_database=config_value_strategy,
        mysql_user=config_value_strategy,
        mysql_password=config_value_strategy,
        port=port_strategy,
        batch_size=batch_size_strategy
    )
    @settings(max_examples=20)
    def test_optional_settings_use_defaults_when_not_provided(
        self, client_id, client_secret, mysql_host, mysql_database, 
        mysql_user, mysql_password, port, batch_size
    ):
        """
        Feature: trestle-etl-pipeline, Property 20: Configuration Loading Completeness
        
        For any valid configuration with only required settings, optional settings
        should use sensible defaults.
        """
        config_dict = {
            "TRESTLE_CLIENT_ID": client_id,
            "TRESTLE_CLIENT_SECRET": client_secret,
            "MYSQL_HOST": mysql_host,
            "MYSQL_DATABASE": mysql_database,
            "MYSQL_USER": mysql_user,
            "MYSQL_PASSWORD": mysql_password,
        }
        
        env_path = create_env_file(config_dict)
        try:
            clear_env_vars()  # Clear before loading
            manager = ConfigManager()
            config = manager.load_config(env_path)
            
            # Verify defaults are applied for optional settings
            assert config.database.port == 3306  # Default port
            assert config.database.charset == "utf8mb4"  # Default charset
            assert config.etl.batch_size == 1000  # Default batch size
            assert config.etl.max_retries == 3  # Default retries
            assert config.etl.log_level == "INFO"  # Default log level
            assert config.api.timeout == 30  # Default timeout
        finally:
            cleanup_env_file(env_path)
            clear_env_vars()

    @given(
        client_id=config_value_strategy,
        client_secret=config_value_strategy,
        mysql_host=config_value_strategy,
        mysql_database=config_value_strategy,
        mysql_user=config_value_strategy,
        mysql_password=config_value_strategy,
        custom_port=port_strategy,
        custom_batch_size=batch_size_strategy,
        custom_retries=st.integers(min_value=1, max_value=10)
    )
    @settings(max_examples=20)
    def test_custom_optional_settings_override_defaults(
        self, client_id, client_secret, mysql_host, mysql_database,
        mysql_user, mysql_password, custom_port, custom_batch_size, custom_retries
    ):
        """
        Feature: trestle-etl-pipeline, Property 20: Configuration Loading Completeness
        
        For any valid configuration with custom optional settings, those settings
        should override the defaults.
        """
        config_dict = {
            "TRESTLE_CLIENT_ID": client_id,
            "TRESTLE_CLIENT_SECRET": client_secret,
            "MYSQL_HOST": mysql_host,
            "MYSQL_DATABASE": mysql_database,
            "MYSQL_USER": mysql_user,
            "MYSQL_PASSWORD": mysql_password,
            "MYSQL_PORT": str(custom_port),
            "BATCH_SIZE": str(custom_batch_size),
            "MAX_RETRIES": str(custom_retries),
        }
        
        env_path = create_env_file(config_dict)
        try:
            clear_env_vars()  # Clear before loading
            manager = ConfigManager()
            config = manager.load_config(env_path)
            
            # Verify custom settings override defaults
            assert config.database.port == custom_port
            assert config.etl.batch_size == custom_batch_size
            assert config.etl.max_retries == custom_retries
        finally:
            cleanup_env_file(env_path)
            clear_env_vars()

    @given(
        missing_var=st.sampled_from([
            "TRESTLE_CLIENT_ID", "TRESTLE_CLIENT_SECRET",
            "MYSQL_HOST", "MYSQL_DATABASE", "MYSQL_USER", "MYSQL_PASSWORD"
        ])
    )
    @settings(max_examples=10)
    def test_missing_required_vars_raises_clear_error(self, missing_var):
        """
        Feature: trestle-etl-pipeline, Property 20: Configuration Loading Completeness
        
        For any configuration missing a required variable, the ConfigManager
        should raise a clear error message identifying the missing variable.
        """
        # Create config with all required vars except one
        all_required = {
            "TRESTLE_CLIENT_ID": "test_id",
            "TRESTLE_CLIENT_SECRET": "test_secret",
            "MYSQL_HOST": "localhost",
            "MYSQL_DATABASE": "test_db",
            "MYSQL_USER": "test_user",
            "MYSQL_PASSWORD": "test_pass",
        }
        
        # Remove the variable we're testing
        config_dict = {k: v for k, v in all_required.items() if k != missing_var}
        
        env_path = create_env_file(config_dict)
        try:
            clear_env_vars()  # Clear before loading
            manager = ConfigManager()
            with pytest.raises(ConfigurationError) as exc_info:
                manager.load_config(env_path)
            
            # Verify error message identifies the missing variable
            assert missing_var in str(exc_info.value)
        finally:
            cleanup_env_file(env_path)
            clear_env_vars()

    @given(
        client_id=config_value_strategy,
        client_secret=config_value_strategy,
        mysql_host=config_value_strategy,
        mysql_database=config_value_strategy,
        mysql_user=config_value_strategy,
        mysql_password=config_value_strategy
    )
    @settings(max_examples=20)
    def test_config_reload_updates_values(
        self, client_id, client_secret, mysql_host, mysql_database, mysql_user, mysql_password
    ):
        """
        Feature: trestle-etl-pipeline, Property 20: Configuration Loading Completeness
        
        For any configuration, reloading should update values without requiring
        code changes (Requirement 7.3).
        """
        # Initial config
        initial_config = {
            "TRESTLE_CLIENT_ID": "initial_id",
            "TRESTLE_CLIENT_SECRET": "initial_secret",
            "MYSQL_HOST": "initial_host",
            "MYSQL_DATABASE": "initial_db",
            "MYSQL_USER": "initial_user",
            "MYSQL_PASSWORD": "initial_pass",
        }
        
        # Updated config
        updated_config = {
            "TRESTLE_CLIENT_ID": client_id,
            "TRESTLE_CLIENT_SECRET": client_secret,
            "MYSQL_HOST": mysql_host,
            "MYSQL_DATABASE": mysql_database,
            "MYSQL_USER": mysql_user,
            "MYSQL_PASSWORD": mysql_password,
        }
        
        initial_path = create_env_file(initial_config)
        updated_path = create_env_file(updated_config)
        
        try:
            clear_env_vars()  # Clear before loading
            manager = ConfigManager()
            
            # Load initial config
            config1 = manager.load_config(initial_path)
            assert config1.api.client_id == "initial_id"
            
            # Clear env vars before reload to simulate fresh load
            clear_env_vars()
            
            # Reload with updated config
            config2 = manager.reload_config(updated_path)
            assert config2.api.client_id == client_id
            assert config2.api.client_secret == client_secret
            assert config2.database.host == mysql_host
        finally:
            cleanup_env_file(initial_path)
            cleanup_env_file(updated_path)
            clear_env_vars()

    def test_nonexistent_env_file_raises_error(self):
        """
        Feature: trestle-etl-pipeline, Property 20: Configuration Loading Completeness
        
        When a specified env file does not exist, a clear error should be raised.
        """
        manager = ConfigManager()
        with pytest.raises(ConfigurationError) as exc_info:
            manager.load_config("/nonexistent/path/.env")
        
        assert "not found" in str(exc_info.value).lower()

    @given(
        client_id=config_value_strategy,
        client_secret=config_value_strategy,
        mysql_host=config_value_strategy,
        mysql_database=config_value_strategy,
        mysql_user=config_value_strategy,
        mysql_password=config_value_strategy
    )
    @settings(max_examples=20)
    def test_credentials_accessible_via_helper_methods(
        self, client_id, client_secret, mysql_host, mysql_database, mysql_user, mysql_password
    ):
        """
        Feature: trestle-etl-pipeline, Property 20: Configuration Loading Completeness
        
        For any valid configuration, credentials should be securely accessible
        via helper methods (Requirement 7.2).
        """
        config_dict = {
            "TRESTLE_CLIENT_ID": client_id,
            "TRESTLE_CLIENT_SECRET": client_secret,
            "MYSQL_HOST": mysql_host,
            "MYSQL_DATABASE": mysql_database,
            "MYSQL_USER": mysql_user,
            "MYSQL_PASSWORD": mysql_password,
        }
        
        env_path = create_env_file(config_dict)
        try:
            clear_env_vars()  # Clear before loading
            manager = ConfigManager()
            manager.load_config(env_path)
            
            # Test get_api_credentials
            api_id, api_secret = manager.get_api_credentials()
            assert api_id == client_id
            assert api_secret == client_secret
            
            # Test get_database_config
            db_config = manager.get_database_config()
            assert db_config.host == mysql_host
            assert db_config.user == mysql_user
            assert db_config.password == mysql_password
            
            # Test get_schedule_config
            etl_config = manager.get_schedule_config()
            assert etl_config is not None
        finally:
            cleanup_env_file(env_path)
            clear_env_vars()

    def test_helper_methods_raise_error_before_load(self):
        """
        Feature: trestle-etl-pipeline, Property 20: Configuration Loading Completeness
        
        Helper methods should raise clear errors if called before config is loaded.
        """
        manager = ConfigManager()
        
        with pytest.raises(ConfigurationError) as exc_info:
            manager.get_api_credentials()
        assert "not loaded" in str(exc_info.value).lower()
        
        with pytest.raises(ConfigurationError) as exc_info:
            manager.get_database_config()
        assert "not loaded" in str(exc_info.value).lower()
        
        with pytest.raises(ConfigurationError) as exc_info:
            manager.get_schedule_config()
        assert "not loaded" in str(exc_info.value).lower()
