"""
Main ETL Orchestration Script for Trestle ETL Pipeline.

Coordinates all ETL phases: extraction, transformation, and loading.
Provides command-line interface for different execution modes.

Requirements: 5.2, 5.3, 5.5
"""

import argparse
import atexit
import fcntl
import os
import signal
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

from .config import ConfigManager, Config, ConfigurationError, validate_config_on_startup
from .odata_client import ODataClient, ODataError, RateLimitError
from .data_transformer import DataTransformer, DataTransformationError, ValidationError
from .mysql_loader import MySQLLoader, DatabaseError, SyncStatus, SyncRun
from .incremental_sync import IncrementalSyncManager, DataType, IncrementalSyncError
from .cost_monitor import CostMonitor, AlertLevel
from .logger import ETLLogger, create_etl_logger
from .alerting import AlertManager, create_alert_manager_from_config


class ETLExecutionError(Exception):
    """Raised when ETL execution fails."""
    pass


class LockAcquisitionError(Exception):
    """Raised when lock acquisition fails."""
    pass


class ResourceConstraintError(Exception):
    """Raised when system resources are constrained."""
    pass


class ResourceMonitor:
    """
    Monitors system resources and provides graceful degradation.
    
    Tracks memory usage, disk space, and provides recommendations
    for batch size adjustments based on available resources.
    """
    
    # Thresholds for resource warnings
    MEMORY_WARNING_PERCENT = 80
    MEMORY_CRITICAL_PERCENT = 90
    DISK_WARNING_PERCENT = 90
    DISK_CRITICAL_PERCENT = 95
    
    def __init__(self, logger: Optional['ETLLogger'] = None):
        """
        Initialize ResourceMonitor.
        
        Args:
            logger: Optional ETL logger for resource warnings.
        """
        self.logger = logger
        self._initial_memory = self._get_memory_usage()
    
    def _get_memory_usage(self) -> Dict[str, Any]:
        """Get current memory usage statistics."""
        try:
            import psutil
            mem = psutil.virtual_memory()
            return {
                'total': mem.total,
                'available': mem.available,
                'percent': mem.percent,
                'used': mem.used
            }
        except ImportError:
            # psutil not available, return defaults
            return {
                'total': 0,
                'available': 0,
                'percent': 0,
                'used': 0
            }
    
    def _get_disk_usage(self, path: str = '/') -> Dict[str, Any]:
        """Get disk usage for the specified path."""
        try:
            import psutil
            disk = psutil.disk_usage(path)
            return {
                'total': disk.total,
                'free': disk.free,
                'percent': disk.percent,
                'used': disk.used
            }
        except ImportError:
            # psutil not available, return defaults
            return {
                'total': 0,
                'free': 0,
                'percent': 0,
                'used': 0
            }
    
    def check_resources(self) -> Dict[str, Any]:
        """
        Check current resource status.
        
        Returns:
            Dictionary with resource status and recommendations.
        """
        memory = self._get_memory_usage()
        disk = self._get_disk_usage()
        
        status = {
            'memory': memory,
            'disk': disk,
            'warnings': [],
            'critical': False,
            'recommended_batch_size_factor': 1.0
        }
        
        # Check memory
        if memory['percent'] >= self.MEMORY_CRITICAL_PERCENT:
            status['warnings'].append(f"Critical memory usage: {memory['percent']:.1f}%")
            status['critical'] = True
            status['recommended_batch_size_factor'] = 0.25
        elif memory['percent'] >= self.MEMORY_WARNING_PERCENT:
            status['warnings'].append(f"High memory usage: {memory['percent']:.1f}%")
            status['recommended_batch_size_factor'] = 0.5
        
        # Check disk
        if disk['percent'] >= self.DISK_CRITICAL_PERCENT:
            status['warnings'].append(f"Critical disk usage: {disk['percent']:.1f}%")
            status['critical'] = True
        elif disk['percent'] >= self.DISK_WARNING_PERCENT:
            status['warnings'].append(f"High disk usage: {disk['percent']:.1f}%")
        
        # Log warnings
        if self.logger and status['warnings']:
            for warning in status['warnings']:
                self.logger.log_error(f"Resource warning: {warning}")
        
        return status
    
    def get_recommended_batch_size(self, default_batch_size: int) -> int:
        """
        Get recommended batch size based on available resources.
        
        Args:
            default_batch_size: The default batch size from configuration.
            
        Returns:
            Recommended batch size (may be reduced if resources are constrained).
        """
        status = self.check_resources()
        factor = status['recommended_batch_size_factor']
        
        recommended = int(default_batch_size * factor)
        
        # Ensure minimum batch size
        return max(recommended, 100)
    
    def should_continue(self) -> bool:
        """
        Check if ETL should continue based on resource status.
        
        Returns:
            True if resources are sufficient to continue.
        """
        status = self.check_resources()
        return not status['critical']
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get performance metrics for monitoring.
        
        Returns:
            Dictionary with performance metrics.
        """
        current_memory = self._get_memory_usage()
        
        return {
            'memory_used_mb': current_memory['used'] / (1024 * 1024) if current_memory['used'] else 0,
            'memory_percent': current_memory['percent'],
            'memory_delta_mb': (current_memory['used'] - self._initial_memory.get('used', 0)) / (1024 * 1024) if current_memory['used'] else 0
        }


class ExecutionLock:
    """
    File-based execution lock to prevent concurrent ETL runs.
    
    Uses fcntl for POSIX-compliant file locking.
    """
    
    def __init__(self, lock_file_path: str):
        """
        Initialize execution lock.
        
        Args:
            lock_file_path: Path to the lock file.
        """
        self.lock_file_path = Path(lock_file_path)
        self._lock_file = None
        self._locked = False
    
    def acquire(self) -> bool:
        """
        Attempt to acquire the execution lock.
        
        Returns:
            True if lock was acquired, False otherwise.
        """
        try:
            # Ensure parent directory exists
            self.lock_file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Open lock file (create if doesn't exist)
            self._lock_file = open(self.lock_file_path, 'w')
            
            # Try to acquire exclusive lock (non-blocking)
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            
            # Write PID to lock file for debugging
            self._lock_file.write(f"{os.getpid()}\n")
            self._lock_file.write(f"{datetime.now().isoformat()}\n")
            self._lock_file.flush()
            
            self._locked = True
            return True
            
        except (IOError, OSError):
            # Lock is held by another process
            if self._lock_file:
                self._lock_file.close()
                self._lock_file = None
            return False
    
    def release(self) -> None:
        """Release the execution lock."""
        if self._lock_file:
            try:
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
                self._lock_file.close()
            except (IOError, OSError):
                pass
            finally:
                self._lock_file = None
                self._locked = False
    
    def is_locked(self) -> bool:
        """Check if lock is currently held."""
        return self._locked
    
    def get_lock_info(self) -> Optional[Dict[str, str]]:
        """
        Get information about the current lock holder.
        
        Returns:
            Dictionary with PID and timestamp, or None if no lock.
        """
        if not self.lock_file_path.exists():
            return None
        
        try:
            with open(self.lock_file_path, 'r') as f:
                lines = f.readlines()
                if len(lines) >= 2:
                    return {
                        'pid': lines[0].strip(),
                        'timestamp': lines[1].strip()
                    }
        except (IOError, OSError):
            pass
        return None
    
    def __enter__(self):
        """Context manager entry."""
        if not self.acquire():
            lock_info = self.get_lock_info()
            if lock_info:
                raise LockAcquisitionError(
                    f"ETL is already running (PID: {lock_info['pid']}, "
                    f"started: {lock_info['timestamp']})"
                )
            raise LockAcquisitionError("ETL is already running")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.release()


class ETLOrchestrator:
    """
    Main ETL orchestration class.
    
    Coordinates all ETL phases with proper error handling,
    retry logic, and resource management.
    """
    
    def __init__(
        self,
        config: Config,
        logger: Optional[ETLLogger] = None,
        alert_manager: Optional[AlertManager] = None
    ):
        """
        Initialize ETL orchestrator.
        
        Args:
            config: Configuration object.
            logger: Optional ETL logger.
            alert_manager: Optional alert manager.
        """
        self.config = config
        self.logger = logger or create_etl_logger(
            log_dir=config.etl.log_dir,
            log_level=config.etl.log_level
        )
        self.alert_manager = alert_manager
        
        # Initialize components (lazy initialization)
        self._odata_client: Optional[ODataClient] = None
        self._data_transformer: Optional[DataTransformer] = None
        self._mysql_loader: Optional[MySQLLoader] = None
        self._incremental_sync: Optional[IncrementalSyncManager] = None
        self._cost_monitor: Optional[CostMonitor] = None
        
        # Execution state
        self._run_id: Optional[str] = None
        self._sync_run: Optional[SyncRun] = None
        self._start_time: Optional[datetime] = None
        self._should_stop = False

    def _init_components(self) -> None:
        """Initialize all ETL components."""
        # OData Client
        self._odata_client = ODataClient(
            config=self.config.api,
            max_retries=self.config.etl.max_retries
        )
        
        # Data Transformer
        import logging
        self._data_transformer = DataTransformer(
            logger=logging.getLogger('trestle_etl.data_transformer')
        )
        
        # MySQL Loader
        self._mysql_loader = MySQLLoader(
            config=self.config.database,
            batch_size=self.config.etl.batch_size,
            max_retries=self.config.etl.max_retries
        )
        
        # Cost Monitor with alert callback
        def alert_callback(level: AlertLevel, message: str):
            if self.alert_manager and level in (AlertLevel.WARNING, AlertLevel.CRITICAL):
                if level == AlertLevel.CRITICAL:
                    self.alert_manager.alert_quota_critical(
                        "API", 0, 0, 0  # Will be filled by actual quota info
                    )
        
        self._cost_monitor = CostMonitor(
            alert_callback=alert_callback
        )
        
        # Incremental Sync Manager
        self._incremental_sync = IncrementalSyncManager(
            odata_client=self._odata_client,
            mysql_loader=self._mysql_loader,
            incremental_field=self.config.etl.incremental_field,
            page_size=self.config.etl.batch_size,
            throttle_seconds=self.config.etl.throttle_seconds
        )
    
    def _cleanup_components(self) -> None:
        """Clean up all ETL components."""
        if self._odata_client:
            self._odata_client.close()
        if self._mysql_loader:
            self._mysql_loader.close_connection()
        if self.logger:
            self.logger.close()
    
    def _setup_signal_handlers(self) -> None:
        """Set up signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            self.logger.log_error(f"Received signal {signum}, initiating graceful shutdown")
            self._should_stop = True
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
    
    def run(
        self,
        full_sync: bool = False,
        data_types: Optional[List[str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Execute the ETL pipeline.
        
        Args:
            full_sync: If True, perform full sync instead of incremental.
            data_types: List of data types to sync (default: Property only).
            dry_run: If True, don't write to database.
            
        Returns:
            Dictionary with execution results.
        """
        self._run_id = str(uuid.uuid4())[:8]
        self._start_time = datetime.now()
        
        results = {
            'run_id': self._run_id,
            'start_time': self._start_time.isoformat(),
            'status': 'success',
            'records_processed': 0,
            'records_inserted': 0,
            'records_updated': 0,
            'api_calls': 0,
            'errors': []
        }
        
        try:
            # Initialize components
            self._init_components()
            self._setup_signal_handlers()
            
            # Log ETL start
            config_summary = {
                'full_sync': full_sync,
                'data_types': data_types or ['Property'],
                'dry_run': dry_run,
                'batch_size': self.config.etl.batch_size
            }
            self.logger.log_etl_start(self._run_id, config_summary)
            
            # Initialize database schema if needed
            if not dry_run:
                self._mysql_loader.initialize_schema()
                self._sync_run = self._mysql_loader.start_sync_run()
            
            # Determine data types to sync
            sync_data_types = [DataType.PROPERTY]
            if data_types:
                sync_data_types = [DataType(dt) for dt in data_types if hasattr(DataType, dt.upper())]
            
            # Execute sync
            if self._should_stop:
                raise ETLExecutionError("ETL stopped by signal")
            
            batch_result = self._incremental_sync.execute_batched_sync(
                data_types=sync_data_types,
                use_incremental=not full_sync,
                use_expand=self.config.etl.use_expand
            )
            
            # Process results for each data type
            for data_type, sync_result in batch_result.results.items():
                if sync_result.records_fetched > 0:
                    # Transform data
                    self.logger.log_transformation_start(sync_result.records_fetched)
                    
                    # Get raw records from API (re-fetch for transformation)
                    # In a real implementation, we'd cache these from the sync
                    records = self._fetch_records_for_type(
                        data_type, 
                        full_sync,
                        sync_result.records_fetched
                    )
                    
                    if records and not dry_run:
                        # Transform records
                        transform_result = self._data_transformer.transform_batch(
                            records,
                            continue_on_error=True
                        )
                        
                        self.logger.log_transformation_stats(
                            total_records=transform_result['stats'].total_records,
                            valid_records=transform_result['stats'].valid_records,
                            invalid_records=transform_result['stats'].invalid_records,
                            duplicates_found=transform_result['stats'].duplicates_detected
                        )
                        
                        # Load to database
                        if transform_result['records']:
                            load_result = self._mysql_loader.batch_upsert_with_tracking(
                                transform_result['records']
                            )
                            
                            results['records_inserted'] += load_result.inserted
                            results['records_updated'] += load_result.updated
                    
                    results['records_processed'] += sync_result.records_fetched
                
                results['api_calls'] += sync_result.api_calls_made
                
                if sync_result.errors:
                    results['errors'].extend(sync_result.errors)
            
            # Complete sync run
            if self._sync_run and not dry_run:
                self._sync_run.last_sync_timestamp = datetime.now()
                self._mysql_loader.complete_sync_run(
                    self._sync_run,
                    status=SyncStatus.SUCCESS if not results['errors'] else SyncStatus.PARTIAL,
                    error_message='; '.join(results['errors'][:5]) if results['errors'] else None
                )
            
            # Log completion
            metrics = self.logger.log_etl_complete(
                self._run_id,
                results['records_processed'],
                results['records_inserted'],
                results['records_updated'],
                status='success' if not results['errors'] else 'partial'
            )
            
            results['duration_seconds'] = metrics.duration_seconds
            results['end_time'] = datetime.now().isoformat()
            
        except Exception as e:
            results['status'] = 'failed'
            results['errors'].append(str(e))
            results['end_time'] = datetime.now().isoformat()
            
            self.logger.log_critical(f"ETL execution failed: {e}", error=e)
            
            # Complete sync run with failure
            if self._sync_run:
                try:
                    self._mysql_loader.complete_sync_run(
                        self._sync_run,
                        status=SyncStatus.FAILED,
                        error_message=str(e)
                    )
                except Exception:
                    pass
            
            # Send alert
            if self.alert_manager:
                self.alert_manager.alert_etl_critical(
                    str(e),
                    type(e).__name__,
                    {'run_id': self._run_id}
                )
            
            raise ETLExecutionError(f"ETL execution failed: {e}") from e
        
        finally:
            self._cleanup_components()
        
        return results
    
    def _fetch_records_for_type(
        self,
        data_type: DataType,
        full_sync: bool,
        expected_count: int
    ) -> List[Dict[str, Any]]:
        """Fetch records for a specific data type."""
        last_sync = None if full_sync else self._mysql_loader.get_last_sync_timestamp()
        
        filter_expr = None
        if last_sync:
            timestamp_str = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
            filter_expr = f"{self.config.etl.incremental_field} gt {timestamp_str}"
        
        try:
            records = self._odata_client.execute_paginated_query(
                entity_set=data_type.value,
                filter_expr=filter_expr,
                top=self.config.etl.batch_size
            )
            return records
        except ODataError as e:
            self.logger.log_api_error(data_type.value, e)
            return []


def run_etl_with_retry(
    config: Config,
    max_retries: int = 3,
    base_delay: float = 60.0,
    full_sync: bool = False,
    data_types: Optional[List[str]] = None,
    dry_run: bool = False,
    check_resources: bool = True
) -> Dict[str, Any]:
    """
    Run ETL with configurable retry logic and resource monitoring.
    
    Implements exponential backoff retry logic for the entire ETL process,
    with resource constraint handling and graceful degradation.
    
    Args:
        config: Configuration object.
        max_retries: Maximum number of retry attempts.
        base_delay: Base delay in seconds between retries.
        full_sync: If True, perform full sync.
        data_types: List of data types to sync.
        dry_run: If True, don't write to database.
        check_resources: If True, monitor system resources.
        
    Returns:
        Dictionary with execution results.
    """
    logger = create_etl_logger(
        log_dir=config.etl.log_dir,
        log_level=config.etl.log_level
    )
    
    # Create alert manager from config
    alert_manager = create_alert_manager_from_config(
        email_enabled=config.alert.email_enabled,
        email_recipients=config.alert.email_recipients,
        smtp_host=config.alert.smtp_host,
        smtp_port=config.alert.smtp_port,
        smtp_user=config.alert.smtp_user,
        smtp_password=config.alert.smtp_password,
        webhook_enabled=config.alert.webhook_enabled,
        webhook_url=config.alert.webhook_url
    )
    
    # Initialize resource monitor
    resource_monitor = ResourceMonitor(logger=logger) if check_resources else None
    
    # Check initial resources
    if resource_monitor:
        resource_status = resource_monitor.check_resources()
        if resource_status['critical']:
            logger.log_critical(
                "Critical resource constraints detected before ETL start",
                context={'warnings': resource_status['warnings']}
            )
            return {
                'status': 'failed',
                'error': 'Critical resource constraints: ' + '; '.join(resource_status['warnings']),
                'attempts': 0
            }
        
        # Adjust batch size if needed
        recommended_batch = resource_monitor.get_recommended_batch_size(config.etl.batch_size)
        if recommended_batch < config.etl.batch_size:
            logger.log_error(
                f"Reducing batch size from {config.etl.batch_size} to {recommended_batch} "
                f"due to resource constraints"
            )
            config.etl.batch_size = recommended_batch
    
    last_error = None
    attempt_results = []
    
    for attempt in range(max_retries + 1):
        attempt_start = datetime.now()
        
        # Check resources before each attempt
        if resource_monitor and attempt > 0:
            if not resource_monitor.should_continue():
                logger.log_critical(
                    f"Stopping ETL after attempt {attempt} due to resource constraints"
                )
                break
        
        try:
            orchestrator = ETLOrchestrator(
                config=config,
                logger=logger,
                alert_manager=alert_manager
            )
            
            result = orchestrator.run(
                full_sync=full_sync,
                data_types=data_types,
                dry_run=dry_run
            )
            
            # Add performance metrics
            if resource_monitor:
                result['performance_metrics'] = resource_monitor.get_performance_metrics()
            
            result['attempts'] = attempt + 1
            return result
            
        except ETLExecutionError as e:
            last_error = e
            attempt_duration = (datetime.now() - attempt_start).total_seconds()
            
            attempt_results.append({
                'attempt': attempt + 1,
                'error': str(e),
                'duration_seconds': attempt_duration
            })
            
            if attempt < max_retries:
                # Calculate exponential backoff delay with jitter
                import random
                delay = base_delay * (2 ** attempt)
                jitter = delay * 0.25 * (2 * random.random() - 1)
                delay = max(0, delay + jitter)
                
                logger.log_error(
                    f"ETL attempt {attempt + 1} failed, retrying in {delay:.0f}s: {e}",
                    context={
                        'attempt': attempt + 1,
                        'max_retries': max_retries,
                        'delay_seconds': delay
                    }
                )
                time.sleep(delay)
            else:
                logger.log_critical(
                    f"ETL failed after {max_retries + 1} attempts: {e}",
                    error=e,
                    context={'attempt_results': attempt_results}
                )
    
    # All retries exhausted
    final_result = {
        'status': 'failed',
        'error': str(last_error),
        'attempts': len(attempt_results),
        'attempt_details': attempt_results
    }
    
    if resource_monitor:
        final_result['performance_metrics'] = resource_monitor.get_performance_metrics()
    
    return final_result


def create_argument_parser() -> argparse.ArgumentParser:
    """Create command-line argument parser."""
    parser = argparse.ArgumentParser(
        description='Trestle ETL Pipeline - Extract real estate data from CoreLogic Trestle API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run incremental sync (default)
  python -m trestle_etl.main
  
  # Run full sync
  python -m trestle_etl.main --full-sync
  
  # Dry run (no database writes)
  python -m trestle_etl.main --dry-run
  
  # Use specific config file
  python -m trestle_etl.main --config /path/to/.env
  
  # Sync specific data types
  python -m trestle_etl.main --data-types Property Media
        """
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        default=None,
        help='Path to .env configuration file'
    )
    
    parser.add_argument(
        '--full-sync', '-f',
        action='store_true',
        help='Perform full sync instead of incremental'
    )
    
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Dry run - do not write to database'
    )
    
    parser.add_argument(
        '--data-types', '-d',
        nargs='+',
        choices=['Property', 'Media', 'Member', 'Office'],
        default=['Property'],
        help='Data types to sync (default: Property)'
    )
    
    parser.add_argument(
        '--max-retries', '-r',
        type=int,
        default=3,
        help='Maximum retry attempts (default: 3)'
    )
    
    parser.add_argument(
        '--no-lock',
        action='store_true',
        help='Skip execution lock (use with caution)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='Trestle ETL Pipeline v1.0.0'
    )
    
    return parser


def main(args: Optional[List[str]] = None) -> int:
    """
    Main entry point for ETL execution.
    
    Args:
        args: Command-line arguments (uses sys.argv if None).
        
    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    parser = create_argument_parser()
    parsed_args = parser.parse_args(args)
    
    # Load configuration
    try:
        config_manager = ConfigManager(env_file=parsed_args.config)
        config = config_manager.load_config()
    except ConfigurationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 1
    
    # Validate configuration on startup (Requirement 7.3)
    try:
        validation_report = validate_config_on_startup(config, raise_on_error=True)
        if validation_report.get('warnings') and parsed_args.verbose:
            print("Configuration warnings:")
            for warning in validation_report['warnings']:
                print(f"  ⚠️  {warning}")
    except ConfigurationError as e:
        print(f"Configuration validation failed: {e}", file=sys.stderr)
        return 1
    
    # Set log level based on verbose flag
    if parsed_args.verbose:
        config.etl.log_level = 'DEBUG'
    
    # Acquire execution lock
    lock = None
    if not parsed_args.no_lock:
        lock = ExecutionLock(config.etl.lock_file_path)
        try:
            if not lock.acquire():
                lock_info = lock.get_lock_info()
                if lock_info:
                    print(
                        f"ETL is already running (PID: {lock_info['pid']}, "
                        f"started: {lock_info['timestamp']})",
                        file=sys.stderr
                    )
                else:
                    print("ETL is already running", file=sys.stderr)
                return 2
        except Exception as e:
            print(f"Failed to acquire lock: {e}", file=sys.stderr)
            return 2
    
    # Register cleanup
    def cleanup():
        if lock:
            lock.release()
    
    atexit.register(cleanup)
    
    try:
        # Run ETL
        result = run_etl_with_retry(
            config=config,
            max_retries=parsed_args.max_retries,
            full_sync=parsed_args.full_sync,
            data_types=parsed_args.data_types,
            dry_run=parsed_args.dry_run
        )
        
        # Print summary
        if parsed_args.verbose or result.get('status') != 'success':
            print(f"\nETL Execution Summary:")
            print(f"  Run ID: {result.get('run_id', 'N/A')}")
            print(f"  Status: {result.get('status', 'unknown')}")
            print(f"  Records Processed: {result.get('records_processed', 0)}")
            print(f"  Records Inserted: {result.get('records_inserted', 0)}")
            print(f"  Records Updated: {result.get('records_updated', 0)}")
            print(f"  API Calls: {result.get('api_calls', 0)}")
            if result.get('duration_seconds'):
                print(f"  Duration: {result['duration_seconds']:.2f}s")
            if result.get('errors'):
                print(f"  Errors: {len(result['errors'])}")
                for error in result['errors'][:5]:
                    print(f"    - {error}")
        
        return 0 if result.get('status') == 'success' else 1
        
    except KeyboardInterrupt:
        print("\nETL interrupted by user", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"ETL failed: {e}", file=sys.stderr)
        return 1
    finally:
        cleanup()


if __name__ == '__main__':
    sys.exit(main())
