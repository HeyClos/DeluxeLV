"""
Configuration Manager for Trestle ETL Pipeline.

Handles loading and validation of environment variables and configuration settings.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv


class ConfigurationError(Exception):
    """Raised when configuration is invalid or incomplete."""
    pass


@dataclass
class APIConfig:
    """API configuration settings."""
    client_id: str
    client_secret: str
    base_url: str = "https://api-prod.corelogic.com/trestle/odata"
    token_url: str = "https://api.cotality.com/trestle/oidc/connect/token"
    timeout: int = 30


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    host: str
    port: int
    database: str
    user: str
    password: str
    charset: str = "utf8mb4"


@dataclass
class ETLConfig:
    """ETL execution configuration settings."""
    batch_size: int = 1000
    max_retries: int = 3
    incremental_field: str = "ModificationTimestamp"
    log_level: str = "INFO"
    lock_file_path: str = "/tmp/trestle_etl.lock"
    log_dir: str = "logs"


@dataclass
class AlertConfig:
    """Alert configuration settings."""
    email_enabled: bool = False
    email_recipients: List[str] = field(default_factory=list)
    webhook_enabled: bool = False
    webhook_url: Optional[str] = None
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None



@dataclass
class Config:
    """Main configuration container."""
    api: APIConfig
    database: DatabaseConfig
    etl: ETLConfig
    alert: AlertConfig


class ConfigManager:
    """
    Manages configuration loading from environment variables.
    
    Supports loading from .env files and environment variables,
    with validation of required settings.
    """
    
    REQUIRED_API_VARS = ["TRESTLE_CLIENT_ID", "TRESTLE_CLIENT_SECRET"]
    REQUIRED_DB_VARS = ["MYSQL_HOST", "MYSQL_DATABASE", "MYSQL_USER", "MYSQL_PASSWORD"]
    
    def __init__(self, env_file: Optional[str] = None):
        """
        Initialize ConfigManager.
        
        Args:
            env_file: Optional path to .env file. If None, looks for .env in current directory.
        """
        self._env_file = env_file
        self._config: Optional[Config] = None
        self._loaded = False
    
    def load_config(self, env_file: Optional[str] = None) -> Config:
        """
        Load configuration from environment variables.
        
        Args:
            env_file: Optional path to .env file to load.
            
        Returns:
            Config object with all settings.
            
        Raises:
            ConfigurationError: If required settings are missing or invalid.
        """
        file_to_load = env_file or self._env_file
        
        if file_to_load:
            env_path = Path(file_to_load)
            if env_path.exists():
                load_dotenv(env_path)
            else:
                raise ConfigurationError(f"Environment file not found: {file_to_load}")
        else:
            # Try to load from default .env file
            load_dotenv()
        
        # Validate required variables
        self._validate_required_vars()
        
        # Build configuration objects
        api_config = self._load_api_config()
        db_config = self._load_database_config()
        etl_config = self._load_etl_config()
        alert_config = self._load_alert_config()
        
        self._config = Config(
            api=api_config,
            database=db_config,
            etl=etl_config,
            alert=alert_config
        )
        self._loaded = True
        
        return self._config
    
    def _validate_required_vars(self) -> None:
        """Validate that all required environment variables are set."""
        missing_vars = []
        
        for var in self.REQUIRED_API_VARS:
            if not os.getenv(var):
                missing_vars.append(var)
        
        for var in self.REQUIRED_DB_VARS:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            raise ConfigurationError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )
    
    def _load_api_config(self) -> APIConfig:
        """Load API configuration from environment."""
        return APIConfig(
            client_id=os.getenv("TRESTLE_CLIENT_ID", ""),
            client_secret=os.getenv("TRESTLE_CLIENT_SECRET", ""),
            base_url=os.getenv("TRESTLE_API_BASE_URL", "https://api-prod.corelogic.com/trestle/odata"),
            token_url=os.getenv("TRESTLE_TOKEN_URL", "https://api.cotality.com/trestle/oidc/connect/token"),
            timeout=int(os.getenv("TRESTLE_API_TIMEOUT", "30"))
        )
    
    def _load_database_config(self) -> DatabaseConfig:
        """Load database configuration from environment."""
        return DatabaseConfig(
            host=os.getenv("MYSQL_HOST", ""),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            database=os.getenv("MYSQL_DATABASE", ""),
            user=os.getenv("MYSQL_USER", ""),
            password=os.getenv("MYSQL_PASSWORD", ""),
            charset=os.getenv("MYSQL_CHARSET", "utf8mb4")
        )
    
    def _load_etl_config(self) -> ETLConfig:
        """Load ETL configuration from environment."""
        return ETLConfig(
            batch_size=int(os.getenv("BATCH_SIZE", "1000")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            incremental_field=os.getenv("INCREMENTAL_FIELD", "ModificationTimestamp"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            lock_file_path=os.getenv("LOCK_FILE_PATH", "/tmp/trestle_etl.lock"),
            log_dir=os.getenv("LOG_DIR", "logs")
        )
    
    def _load_alert_config(self) -> AlertConfig:
        """Load alert configuration from environment."""
        email_recipients_str = os.getenv("ALERT_EMAIL_RECIPIENTS", "")
        email_recipients = [r.strip() for r in email_recipients_str.split(",") if r.strip()]
        
        return AlertConfig(
            email_enabled=os.getenv("ALERT_EMAIL_ENABLED", "false").lower() == "true",
            email_recipients=email_recipients,
            webhook_enabled=os.getenv("ALERT_WEBHOOK_ENABLED", "false").lower() == "true",
            webhook_url=os.getenv("ALERT_WEBHOOK_URL"),
            smtp_host=os.getenv("SMTP_HOST"),
            smtp_port=int(os.getenv("SMTP_PORT", "587")),
            smtp_user=os.getenv("SMTP_USER"),
            smtp_password=os.getenv("SMTP_PASSWORD")
        )
    
    def get_api_credentials(self) -> tuple[str, str]:
        """
        Get API credentials.
        
        Returns:
            Tuple of (client_id, client_secret).
            
        Raises:
            ConfigurationError: If config not loaded.
        """
        if not self._loaded or not self._config:
            raise ConfigurationError("Configuration not loaded. Call load_config() first.")
        return self._config.api.client_id, self._config.api.client_secret
    
    def get_database_config(self) -> DatabaseConfig:
        """
        Get database configuration.
        
        Returns:
            DatabaseConfig object.
            
        Raises:
            ConfigurationError: If config not loaded.
        """
        if not self._loaded or not self._config:
            raise ConfigurationError("Configuration not loaded. Call load_config() first.")
        return self._config.database
    
    def get_schedule_config(self) -> ETLConfig:
        """
        Get ETL execution settings.
        
        Returns:
            ETLConfig object.
            
        Raises:
            ConfigurationError: If config not loaded.
        """
        if not self._loaded or not self._config:
            raise ConfigurationError("Configuration not loaded. Call load_config() first.")
        return self._config.etl
    
    def reload_config(self, env_file: Optional[str] = None) -> Config:
        """
        Reload configuration from environment.
        
        This allows updating configuration without code changes.
        
        Args:
            env_file: Optional path to .env file.
            
        Returns:
            Updated Config object.
        """
        self._loaded = False
        self._config = None
        return self.load_config(env_file)
    
    @property
    def config(self) -> Config:
        """Get the current configuration."""
        if not self._loaded or not self._config:
            raise ConfigurationError("Configuration not loaded. Call load_config() first.")
        return self._config
