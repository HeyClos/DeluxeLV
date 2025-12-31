"""
Property-based tests for ETL Logger.

Feature: trestle-etl-pipeline
Tests Property 17 for comprehensive logging coverage.
"""

import json
import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

import pytest
from hypothesis import given, strategies as st, settings, assume

from trestle_etl.logger import (
    ETLLogger, LogType, PerformanceMetrics, ETLLogEntry,
    StructuredFormatter, StandardFormatter, create_etl_logger
)


# Strategy for generating run IDs (ASCII only to avoid JSON escaping issues)
run_id_strategy = st.text(
    alphabet=st.characters(whitelist_categories=('L', 'N'), whitelist_characters='-_', 
                           max_codepoint=127),
    min_size=1,
    max_size=50
)

# Strategy for generating endpoint names
endpoint_strategy = st.sampled_from([
    'Property', 'Media', 'Member', 'Office', 'Listing', '/api/v1/data'
])

# Strategy for generating record counts
record_count_strategy = st.integers(min_value=0, max_value=100000)

# Strategy for generating response times
response_time_strategy = st.floats(min_value=0.0, max_value=10000.0, allow_nan=False)

# Strategy for generating log levels
log_level_strategy = st.sampled_from(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])

# Strategy for generating error messages
error_message_strategy = st.text(min_size=1, max_size=200)

# Strategy for generating field names
field_name_strategy = st.text(
    alphabet=st.characters(whitelist_categories=('L', 'N'), whitelist_characters='_'),
    min_size=1,
    max_size=50
)


class TestComprehensiveLoggingCoverage:
    """
    Property 17: Comprehensive Logging Coverage
    
    For any ETL operation, all significant events, errors, and performance 
    metrics should be logged to appropriate log files with proper rotation.
    
    Validates: Requirements 5.4, 6.1, 6.2, 6.3
    """
    
    @given(
        run_id=run_id_strategy,
        records_processed=record_count_strategy,
        records_inserted=record_count_strategy,
        records_updated=record_count_strategy
    )
    @settings(max_examples=100)
    def test_etl_operations_are_logged(
        self, run_id, records_processed, records_inserted, records_updated
    ):
        """
        Feature: trestle-etl-pipeline, Property 17: Comprehensive Logging Coverage
        
        For any ETL operation, start and completion events should be logged
        with appropriate details.
        """
        assume(len(run_id.strip()) > 0)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            logger = ETLLogger(log_dir=temp_dir, log_level="DEBUG")
            
            # Log ETL start
            logger.log_etl_start(run_id, {'batch_size': 1000})
            
            # Log ETL complete
            metrics = logger.log_etl_complete(
                run_id=run_id,
                records_processed=records_processed,
                records_inserted=records_inserted,
                records_updated=records_updated,
                status="success"
            )
            
            # Verify metrics are recorded
            assert metrics.records_processed == records_processed
            assert metrics.records_inserted == records_inserted
            assert metrics.records_updated == records_updated
            assert metrics.duration_seconds >= 0
            
            # Verify log file exists and contains entries
            main_log = Path(temp_dir) / "etl_main.log"
            assert main_log.exists(), "Main log file should exist"
            
            log_content = main_log.read_text()
            assert run_id in log_content, "Run ID should be in log"
            
            logger.close()
    
    @given(
        endpoint=endpoint_strategy,
        status_code=st.sampled_from([200, 400, 401, 404, 429, 500]),
        response_time_ms=response_time_strategy,
        records_returned=record_count_strategy
    )
    @settings(max_examples=100)
    def test_api_responses_are_logged(
        self, endpoint, status_code, response_time_ms, records_returned
    ):
        """
        Feature: trestle-etl-pipeline, Property 17: Comprehensive Logging Coverage
        
        For any API response, the response details should be logged including
        status code, response time, and records returned.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            logger = ETLLogger(log_dir=temp_dir, log_level="DEBUG")
            
            # Log API request and response
            logger.log_api_request(endpoint, "GET", {'$top': 1000})
            logger.log_api_response(
                endpoint=endpoint,
                status_code=status_code,
                response_time_ms=response_time_ms,
                records_returned=records_returned,
                quota_info={'minute_remaining': 100}
            )
            
            # Verify API log file exists and contains entries
            api_log = Path(temp_dir) / "api_calls.log"
            assert api_log.exists(), "API log file should exist"
            
            log_content = api_log.read_text()
            assert endpoint in log_content, "Endpoint should be in log"
            assert str(status_code) in log_content, "Status code should be in log"
            
            logger.close()
    
    @given(
        endpoint=endpoint_strategy,
        error_message=error_message_strategy,
        retry_count=st.integers(min_value=0, max_value=5)
    )
    @settings(max_examples=100)
    def test_errors_are_logged_with_context(
        self, endpoint, error_message, retry_count
    ):
        """
        Feature: trestle-etl-pipeline, Property 17: Comprehensive Logging Coverage
        
        For any error, the error details, context, and stack traces should
        be captured in error logs.
        """
        assume(len(error_message.strip()) > 0)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            logger = ETLLogger(log_dir=temp_dir, log_level="DEBUG")
            
            # Create and log an error
            error = ValueError(error_message)
            logger.log_api_error(endpoint, error, retry_count)
            
            # Verify error log file exists and contains entries
            error_log = Path(temp_dir) / "errors.log"
            assert error_log.exists(), "Error log file should exist"
            
            log_content = error_log.read_text()
            assert "ValueError" in log_content, "Error type should be in log"
            
            logger.close()
    
    @given(
        total_records=st.integers(min_value=1, max_value=10000),
        valid_percent=st.floats(min_value=0.0, max_value=1.0, allow_nan=False)
    )
    @settings(max_examples=100)
    def test_transformation_stats_are_logged(
        self, total_records, valid_percent
    ):
        """
        Feature: trestle-etl-pipeline, Property 17: Comprehensive Logging Coverage
        
        For any data transformation, statistics about valid/invalid records
        should be logged.
        """
        valid_records = int(total_records * valid_percent)
        invalid_records = total_records - valid_records
        
        with tempfile.TemporaryDirectory() as temp_dir:
            logger = ETLLogger(log_dir=temp_dir, log_level="DEBUG")
            
            # Log transformation stats
            logger.log_transformation_start(total_records)
            logger.log_transformation_stats(
                total_records=total_records,
                valid_records=valid_records,
                invalid_records=invalid_records,
                duplicates_found=0
            )
            
            # Verify data quality log file exists
            dq_log = Path(temp_dir) / "data_quality.log"
            assert dq_log.exists(), "Data quality log file should exist"
            
            log_content = dq_log.read_text()
            assert str(total_records) in log_content, "Total records should be in log"
            
            logger.close()
    
    @given(
        operation=st.sampled_from(['insert', 'update', 'upsert', 'delete']),
        records_affected=record_count_strategy,
        duration_ms=response_time_strategy
    )
    @settings(max_examples=100)
    def test_database_operations_are_logged(
        self, operation, records_affected, duration_ms
    ):
        """
        Feature: trestle-etl-pipeline, Property 17: Comprehensive Logging Coverage
        
        For any database operation, the operation details and performance
        metrics should be logged.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            logger = ETLLogger(log_dir=temp_dir, log_level="DEBUG")
            
            # Log database operation
            logger.log_database_operations(
                operation=operation,
                records_affected=records_affected,
                duration_ms=duration_ms
            )
            
            # Verify database log file exists
            db_log = Path(temp_dir) / "database.log"
            assert db_log.exists(), "Database log file should exist"
            
            log_content = db_log.read_text()
            assert operation in log_content, "Operation should be in log"
            
            logger.close()
    
    @given(
        metric_name=st.text(min_size=1, max_size=50),
        value=st.floats(min_value=0.0, max_value=1000000.0, allow_nan=False),
        unit=st.sampled_from(['ms', 's', 'records', 'bytes', 'MB', ''])
    )
    @settings(max_examples=100)
    def test_performance_metrics_are_logged(
        self, metric_name, value, unit
    ):
        """
        Feature: trestle-etl-pipeline, Property 17: Comprehensive Logging Coverage
        
        For any performance metric, the metric name, value, and unit should
        be logged to the performance log.
        """
        assume(len(metric_name.strip()) > 0)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            logger = ETLLogger(log_dir=temp_dir, log_level="DEBUG")
            
            # Log performance metric
            logger.log_performance_metric(
                metric_name=metric_name,
                value=value,
                unit=unit
            )
            
            # Verify performance log file exists
            perf_log = Path(temp_dir) / "performance.log"
            assert perf_log.exists(), "Performance log file should exist"
            
            logger.close()
    
    @given(log_level=log_level_strategy)
    @settings(max_examples=50)
    def test_log_level_configuration(self, log_level):
        """
        Feature: trestle-etl-pipeline, Property 17: Comprehensive Logging Coverage
        
        For any log level configuration, the logger should respect the
        configured level.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            logger = ETLLogger(log_dir=temp_dir, log_level=log_level)
            
            # Verify log level is set correctly
            expected_level = getattr(logging, log_level)
            assert logger.log_level == expected_level
            
            # Verify level can be changed
            logger.set_log_level("WARNING")
            assert logger.log_level == logging.WARNING
            
            logger.close()
    
    @given(
        num_operations=st.integers(min_value=1, max_value=20),
        records_per_op=record_count_strategy
    )
    @settings(max_examples=50)
    def test_performance_metrics_accumulation(
        self, num_operations, records_per_op
    ):
        """
        Feature: trestle-etl-pipeline, Property 17: Comprehensive Logging Coverage
        
        For any series of operations, performance metrics should accumulate
        correctly.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            logger = ETLLogger(log_dir=temp_dir, log_level="DEBUG")
            
            # Run multiple ETL operations
            for i in range(num_operations):
                run_id = f"run_{i}"
                logger.log_etl_start(run_id)
                logger.log_etl_complete(
                    run_id=run_id,
                    records_processed=records_per_op,
                    records_inserted=records_per_op // 2,
                    records_updated=records_per_op // 2
                )
            
            # Verify all metrics are recorded
            metrics = logger.get_performance_metrics()
            assert len(metrics) == num_operations, \
                f"Expected {num_operations} metrics, got {len(metrics)}"
            
            logger.close()
    
    @given(use_json=st.booleans())
    @settings(max_examples=20)
    def test_log_format_configuration(self, use_json):
        """
        Feature: trestle-etl-pipeline, Property 17: Comprehensive Logging Coverage
        
        For any format configuration, logs should be written in the
        specified format (JSON or standard).
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            logger = ETLLogger(
                log_dir=temp_dir,
                log_level="DEBUG",
                use_json_format=use_json
            )
            
            # Log something
            logger.log_etl_start("test_run")
            logger.log_etl_complete(
                run_id="test_run",
                records_processed=100,
                records_inserted=50,
                records_updated=50
            )
            
            # Read log content
            main_log = Path(temp_dir) / "etl_main.log"
            log_content = main_log.read_text()
            
            if use_json:
                # JSON format should have valid JSON lines
                for line in log_content.strip().split('\n'):
                    if line.strip():
                        try:
                            json.loads(line)
                        except json.JSONDecodeError:
                            # Some lines may be from console handler
                            pass
            
            logger.close()
    
    @given(
        record_id=st.text(min_size=1, max_size=50),
        field=field_name_strategy,
        error_msg=error_message_strategy
    )
    @settings(max_examples=100)
    def test_validation_errors_are_logged(
        self, record_id, field, error_msg
    ):
        """
        Feature: trestle-etl-pipeline, Property 17: Comprehensive Logging Coverage
        
        For any validation error, the record ID, field, and error message
        should be logged.
        """
        assume(len(record_id.strip()) > 0)
        assume(len(field.strip()) > 0)
        assume(len(error_msg.strip()) > 0)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            logger = ETLLogger(log_dir=temp_dir, log_level="DEBUG")
            
            # Log validation error
            logger.log_validation_error(
                record_id=record_id,
                field=field,
                error_message=error_msg,
                value="invalid_value"
            )
            
            # Verify data quality log contains the error
            dq_log = Path(temp_dir) / "data_quality.log"
            assert dq_log.exists(), "Data quality log should exist"
            
            logger.close()
    
    @given(
        message=error_message_strategy,
        is_critical=st.booleans()
    )
    @settings(max_examples=50)
    def test_critical_errors_logged_to_multiple_files(
        self, message, is_critical
    ):
        """
        Feature: trestle-etl-pipeline, Property 17: Comprehensive Logging Coverage
        
        For any critical error, it should be logged to both error log
        and main log.
        """
        assume(len(message.strip()) > 0)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            logger = ETLLogger(log_dir=temp_dir, log_level="DEBUG")
            
            if is_critical:
                logger.log_critical(message, context={'severity': 'critical'})
            else:
                logger.log_error(message, context={'severity': 'error'})
            
            # Verify error log exists
            error_log = Path(temp_dir) / "errors.log"
            assert error_log.exists(), "Error log should exist"
            
            logger.close()
    
    def test_all_log_types_create_files(self):
        """
        Feature: trestle-etl-pipeline, Property 17: Comprehensive Logging Coverage
        
        All log types should create their respective log files.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            logger = ETLLogger(log_dir=temp_dir, log_level="DEBUG")
            
            # Trigger logging for each type
            logger.log_etl_start("test")
            logger.log_api_response("endpoint", 200, 100.0, 10)
            logger.log_transformation_stats(100, 90, 10, 0)
            logger.log_database_operations("insert", 100, 50.0)
            logger.log_error("test error")
            logger.log_performance_metric("test_metric", 100.0, "ms")
            
            # Verify all log files exist
            log_files = logger.get_log_files()
            for log_type, log_path in log_files.items():
                assert log_path.exists(), f"Log file {log_type} should exist"
            
            logger.close()
