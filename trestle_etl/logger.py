"""
Comprehensive Logging System for Trestle ETL Pipeline.

Provides rotating log files for different log types, structured logging
with appropriate levels, and performance metrics tracking.

Requirements: 5.4, 6.1, 6.2, 6.3
"""

import logging
import os
import json
import traceback
from datetime import datetime
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field, asdict
from enum import Enum


class LogType(Enum):
    """Types of log files."""
    MAIN = "etl_main"
    API = "api_calls"
    DATA_QUALITY = "data_quality"
    DATABASE = "database"
    ERROR = "errors"
    PERFORMANCE = "performance"


@dataclass
class PerformanceMetrics:
    """Performance metrics for an ETL operation."""
    operation: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    records_processed: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_failed: int = 0
    api_calls_made: int = 0
    errors_encountered: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def complete(self) -> None:
        """Mark the operation as complete and calculate duration."""
        self.end_time = datetime.now()
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging."""
        result = asdict(self)
        result['start_time'] = self.start_time.isoformat()
        if self.end_time:
            result['end_time'] = self.end_time.isoformat()
        return result


@dataclass
class ETLLogEntry:
    """Structured log entry for ETL operations."""
    timestamp: datetime
    level: str
    log_type: str
    message: str
    context: Dict[str, Any] = field(default_factory=dict)
    error_details: Optional[Dict[str, Any]] = None
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        data = {
            'timestamp': self.timestamp.isoformat(),
            'level': self.level,
            'log_type': self.log_type,
            'message': self.message,
            'context': self.context
        }
        if self.error_details:
            data['error_details'] = self.error_details
        return json.dumps(data)


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured JSON logging."""
    
    def __init__(self, log_type: str = "general"):
        super().__init__()
        self.log_type = log_type
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'log_type': self.log_type,
            'logger': record.name,
            'message': record.getMessage()
        }
        
        # Add extra context if available
        if hasattr(record, 'context') and record.context:
            log_entry['context'] = record.context
        
        # Add exception info if present
        if record.exc_info:
            log_entry['error_details'] = {
                'exception_type': record.exc_info[0].__name__ if record.exc_info[0] else None,
                'exception_message': str(record.exc_info[1]) if record.exc_info[1] else None,
                'traceback': traceback.format_exception(*record.exc_info) if record.exc_info[0] else None
            }
        
        return json.dumps(log_entry)


class StandardFormatter(logging.Formatter):
    """Standard human-readable formatter."""
    
    def __init__(self):
        super().__init__(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )


class ETLLogger:
    """
    Comprehensive logging system for the Trestle ETL pipeline.
    
    Provides:
    - Rotating log files for different log types
    - Structured JSON logging
    - Performance metrics tracking
    - Error tracking with context
    """
    
    # Default log configuration
    DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
    DEFAULT_BACKUP_COUNT = 5
    DEFAULT_LOG_LEVEL = logging.INFO
    
    def __init__(
        self,
        log_dir: str = "logs",
        log_level: str = "INFO",
        max_bytes: int = DEFAULT_MAX_BYTES,
        backup_count: int = DEFAULT_BACKUP_COUNT,
        use_json_format: bool = True
    ):
        """
        Initialize ETL Logger.
        
        Args:
            log_dir: Directory for log files.
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
            max_bytes: Maximum size of each log file before rotation.
            backup_count: Number of backup files to keep.
            use_json_format: Whether to use JSON structured logging.
        """
        self.log_dir = Path(log_dir)
        self.log_level = getattr(logging, log_level.upper(), logging.INFO)
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.use_json_format = use_json_format
        
        # Create log directory if it doesn't exist
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize loggers for different log types
        self._loggers: Dict[LogType, logging.Logger] = {}
        self._handlers: Dict[LogType, logging.Handler] = {}
        self._performance_metrics: List[PerformanceMetrics] = []
        self._current_operation: Optional[PerformanceMetrics] = None
        
        # Set up all loggers
        self._setup_loggers()
    
    def _setup_loggers(self) -> None:
        """Set up rotating log files for each log type."""
        for log_type in LogType:
            logger = logging.getLogger(f"trestle_etl.{log_type.value}")
            logger.setLevel(self.log_level)
            
            # Remove existing handlers
            logger.handlers.clear()
            
            # Create rotating file handler
            log_file = self.log_dir / f"{log_type.value}.log"
            handler = RotatingFileHandler(
                log_file,
                maxBytes=self.max_bytes,
                backupCount=self.backup_count
            )
            handler.setLevel(self.log_level)
            
            # Set formatter
            if self.use_json_format:
                formatter = StructuredFormatter(log_type.value)
            else:
                formatter = StandardFormatter()
            handler.setFormatter(formatter)
            
            logger.addHandler(handler)
            
            self._loggers[log_type] = logger
            self._handlers[log_type] = handler
        
        # Also add console handler to main logger
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(StandardFormatter())
        self._loggers[LogType.MAIN].addHandler(console_handler)
    
    def _log_with_context(
        self,
        log_type: LogType,
        level: int,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        exc_info: bool = False
    ) -> None:
        """Log a message with optional context."""
        logger = self._loggers.get(log_type)
        if logger:
            extra = {'context': context or {}}
            logger.log(level, message, extra=extra, exc_info=exc_info)
    
    # Main ETL logging methods
    def log_etl_start(self, run_id: str, config_summary: Optional[Dict[str, Any]] = None) -> None:
        """
        Log ETL execution start.
        
        Args:
            run_id: Unique identifier for this ETL run.
            config_summary: Summary of configuration settings.
        """
        context = {
            'run_id': run_id,
            'event': 'etl_start',
            'config': config_summary or {}
        }
        self._log_with_context(
            LogType.MAIN,
            logging.INFO,
            f"ETL execution started - Run ID: {run_id}",
            context
        )
        
        # Start performance tracking
        self._current_operation = PerformanceMetrics(
            operation=f"etl_run_{run_id}",
            start_time=datetime.now(),
            metadata={'run_id': run_id}
        )
    
    def log_etl_complete(
        self,
        run_id: str,
        records_processed: int,
        records_inserted: int,
        records_updated: int,
        status: str = "success"
    ) -> PerformanceMetrics:
        """
        Log ETL execution completion.
        
        Args:
            run_id: Unique identifier for this ETL run.
            records_processed: Total records processed.
            records_inserted: Records inserted.
            records_updated: Records updated.
            status: Completion status.
            
        Returns:
            PerformanceMetrics for the completed run.
        """
        if self._current_operation:
            self._current_operation.records_processed = records_processed
            self._current_operation.records_inserted = records_inserted
            self._current_operation.records_updated = records_updated
            self._current_operation.complete()
            metrics = self._current_operation
            self._performance_metrics.append(metrics)
        else:
            metrics = PerformanceMetrics(
                operation=f"etl_run_{run_id}",
                start_time=datetime.now(),
                records_processed=records_processed,
                records_inserted=records_inserted,
                records_updated=records_updated
            )
            metrics.complete()
        
        context = {
            'run_id': run_id,
            'event': 'etl_complete',
            'status': status,
            'metrics': metrics.to_dict()
        }
        self._log_with_context(
            LogType.MAIN,
            logging.INFO,
            f"ETL execution completed - Run ID: {run_id}, Status: {status}, "
            f"Duration: {metrics.duration_seconds:.2f}s, "
            f"Processed: {records_processed}, Inserted: {records_inserted}, Updated: {records_updated}",
            context
        )
        
        # Also log to performance log
        self._log_with_context(
            LogType.PERFORMANCE,
            logging.INFO,
            f"ETL run completed: {metrics.to_dict()}",
            metrics.to_dict()
        )
        
        self._current_operation = None
        return metrics
    
    # API logging methods
    def log_api_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log API request details."""
        context = {
            'event': 'api_request',
            'endpoint': endpoint,
            'method': method,
            'params': params or {}
        }
        self._log_with_context(
            LogType.API,
            logging.DEBUG,
            f"API Request: {method} {endpoint}",
            context
        )
    
    def log_api_response(
        self,
        endpoint: str,
        status_code: int,
        response_time_ms: float,
        records_returned: int = 0,
        quota_info: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log API response details and performance."""
        context = {
            'event': 'api_response',
            'endpoint': endpoint,
            'status_code': status_code,
            'response_time_ms': response_time_ms,
            'records_returned': records_returned,
            'quota_info': quota_info or {}
        }
        
        level = logging.INFO if status_code == 200 else logging.WARNING
        self._log_with_context(
            LogType.API,
            level,
            f"API Response: {endpoint} - Status: {status_code}, "
            f"Time: {response_time_ms:.2f}ms, Records: {records_returned}",
            context
        )
        
        # Update current operation metrics
        if self._current_operation:
            self._current_operation.api_calls_made += 1
    
    def log_api_error(
        self,
        endpoint: str,
        error: Exception,
        retry_count: int = 0
    ) -> None:
        """Log API error with context."""
        context = {
            'event': 'api_error',
            'endpoint': endpoint,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'retry_count': retry_count
        }
        self._log_with_context(
            LogType.API,
            logging.ERROR,
            f"API Error: {endpoint} - {type(error).__name__}: {error}",
            context,
            exc_info=True
        )
        
        # Also log to error log
        self._log_error(error, context)

    
    # Data transformation logging methods
    def log_transformation_start(self, record_count: int) -> None:
        """Log start of data transformation."""
        context = {
            'event': 'transformation_start',
            'record_count': record_count
        }
        self._log_with_context(
            LogType.DATA_QUALITY,
            logging.INFO,
            f"Starting data transformation for {record_count} records",
            context
        )
    
    def log_transformation_stats(
        self,
        total_records: int,
        valid_records: int,
        invalid_records: int,
        duplicates_found: int = 0
    ) -> None:
        """Log data transformation statistics."""
        context = {
            'event': 'transformation_stats',
            'total_records': total_records,
            'valid_records': valid_records,
            'invalid_records': invalid_records,
            'duplicates_found': duplicates_found,
            'validation_rate': (valid_records / total_records * 100) if total_records > 0 else 0
        }
        self._log_with_context(
            LogType.DATA_QUALITY,
            logging.INFO,
            f"Transformation complete: {valid_records}/{total_records} valid, "
            f"{invalid_records} invalid, {duplicates_found} duplicates",
            context
        )
    
    def log_validation_error(
        self,
        record_id: str,
        field: str,
        error_message: str,
        value: Any = None
    ) -> None:
        """Log data validation error."""
        context = {
            'event': 'validation_error',
            'record_id': record_id,
            'field': field,
            'error_message': error_message,
            'value': str(value) if value is not None else None
        }
        self._log_with_context(
            LogType.DATA_QUALITY,
            logging.WARNING,
            f"Validation error for record {record_id}: {field} - {error_message}",
            context
        )
        
        if self._current_operation:
            self._current_operation.records_failed += 1
    
    def log_duplicate_detected(
        self,
        record_id: str,
        existing_timestamp: Optional[datetime] = None
    ) -> None:
        """Log duplicate record detection."""
        context = {
            'event': 'duplicate_detected',
            'record_id': record_id,
            'existing_timestamp': existing_timestamp.isoformat() if existing_timestamp else None
        }
        self._log_with_context(
            LogType.DATA_QUALITY,
            logging.DEBUG,
            f"Duplicate record detected: {record_id}",
            context
        )
    
    # Database logging methods
    def log_database_connect(self, host: str, database: str) -> None:
        """Log database connection."""
        context = {
            'event': 'db_connect',
            'host': host,
            'database': database
        }
        self._log_with_context(
            LogType.DATABASE,
            logging.INFO,
            f"Connected to database: {database}@{host}",
            context
        )
    
    def log_database_operations(
        self,
        operation: str,
        records_affected: int,
        duration_ms: float
    ) -> None:
        """Log database operation details."""
        context = {
            'event': 'db_operation',
            'operation': operation,
            'records_affected': records_affected,
            'duration_ms': duration_ms
        }
        self._log_with_context(
            LogType.DATABASE,
            logging.INFO,
            f"Database {operation}: {records_affected} records in {duration_ms:.2f}ms",
            context
        )
    
    def log_batch_insert(
        self,
        table: str,
        batch_size: int,
        batch_number: int,
        duration_ms: float
    ) -> None:
        """Log batch insert operation."""
        context = {
            'event': 'batch_insert',
            'table': table,
            'batch_size': batch_size,
            'batch_number': batch_number,
            'duration_ms': duration_ms
        }
        self._log_with_context(
            LogType.DATABASE,
            logging.DEBUG,
            f"Batch insert #{batch_number} to {table}: {batch_size} records in {duration_ms:.2f}ms",
            context
        )
    
    def log_database_error(
        self,
        operation: str,
        error: Exception,
        context_data: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log database error with context."""
        context = {
            'event': 'db_error',
            'operation': operation,
            'error_type': type(error).__name__,
            'error_message': str(error),
            **(context_data or {})
        }
        self._log_with_context(
            LogType.DATABASE,
            logging.ERROR,
            f"Database error during {operation}: {type(error).__name__}: {error}",
            context,
            exc_info=True
        )
        
        # Also log to error log
        self._log_error(error, context)
    
    # Error logging methods
    def _log_error(
        self,
        error: Exception,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log error to error log file."""
        error_context = {
            'event': 'error',
            'error_type': type(error).__name__,
            'error_message': str(error),
            'traceback': traceback.format_exc(),
            **(context or {})
        }
        self._log_with_context(
            LogType.ERROR,
            logging.ERROR,
            f"{type(error).__name__}: {error}",
            error_context,
            exc_info=True
        )
        
        if self._current_operation:
            self._current_operation.errors_encountered += 1
    
    def log_error(
        self,
        message: str,
        error: Optional[Exception] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log an error with optional exception and context."""
        error_context = {
            'event': 'error',
            'message': message,
            **(context or {})
        }
        
        if error:
            error_context['error_type'] = type(error).__name__
            error_context['error_message'] = str(error)
            error_context['traceback'] = traceback.format_exc()
        
        self._log_with_context(
            LogType.ERROR,
            logging.ERROR,
            message,
            error_context,
            exc_info=error is not None
        )
        
        if self._current_operation:
            self._current_operation.errors_encountered += 1
    
    def log_critical(
        self,
        message: str,
        error: Optional[Exception] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log a critical error."""
        error_context = {
            'event': 'critical_error',
            'message': message,
            **(context or {})
        }
        
        if error:
            error_context['error_type'] = type(error).__name__
            error_context['error_message'] = str(error)
            error_context['traceback'] = traceback.format_exc()
        
        self._log_with_context(
            LogType.ERROR,
            logging.CRITICAL,
            message,
            error_context,
            exc_info=error is not None
        )
        
        # Also log to main log
        self._log_with_context(
            LogType.MAIN,
            logging.CRITICAL,
            message,
            error_context,
            exc_info=error is not None
        )
    
    # Performance metrics methods
    def log_performance_metric(
        self,
        metric_name: str,
        value: float,
        unit: str = "",
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log a performance metric."""
        metric_context = {
            'event': 'performance_metric',
            'metric_name': metric_name,
            'value': value,
            'unit': unit,
            **(context or {})
        }
        self._log_with_context(
            LogType.PERFORMANCE,
            logging.INFO,
            f"Performance: {metric_name} = {value}{unit}",
            metric_context
        )
    
    def get_performance_metrics(self) -> List[PerformanceMetrics]:
        """Get all recorded performance metrics."""
        return self._performance_metrics.copy()
    
    def get_current_operation_metrics(self) -> Optional[PerformanceMetrics]:
        """Get metrics for the current operation."""
        return self._current_operation
    
    # Utility methods
    def get_logger(self, log_type: LogType) -> logging.Logger:
        """Get a specific logger by type."""
        return self._loggers.get(log_type, self._loggers[LogType.MAIN])
    
    def set_log_level(self, level: str) -> None:
        """Set log level for all loggers."""
        log_level = getattr(logging, level.upper(), logging.INFO)
        self.log_level = log_level
        
        for logger in self._loggers.values():
            logger.setLevel(log_level)
        
        for handler in self._handlers.values():
            handler.setLevel(log_level)
    
    def get_log_files(self) -> Dict[str, Path]:
        """Get paths to all log files."""
        return {
            log_type.value: self.log_dir / f"{log_type.value}.log"
            for log_type in LogType
        }
    
    def close(self) -> None:
        """Close all log handlers."""
        for handler in self._handlers.values():
            handler.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Convenience function to create a configured logger
def create_etl_logger(
    log_dir: str = "logs",
    log_level: str = "INFO",
    use_json_format: bool = True
) -> ETLLogger:
    """
    Create a configured ETL logger.
    
    Args:
        log_dir: Directory for log files.
        log_level: Logging level.
        use_json_format: Whether to use JSON structured logging.
        
    Returns:
        Configured ETLLogger instance.
    """
    return ETLLogger(
        log_dir=log_dir,
        log_level=log_level,
        use_json_format=use_json_format
    )
