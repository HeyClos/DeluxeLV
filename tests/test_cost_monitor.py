"""
Property-based tests for Cost Monitor.

Feature: trestle-etl-pipeline
Tests Property 18 for cost tracking accuracy.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from unittest.mock import Mock, MagicMock, patch

import pytest
from hypothesis import given, strategies as st, settings, assume

from trestle_etl.cost_monitor import (
    CostMonitor, CostMonitorError, AlertLevel,
    APIUsageRecord, QuotaStatus, UsageReport, CostEstimate
)


# Strategy for generating timestamps
timestamp_strategy = st.datetimes(
    min_value=datetime(2020, 1, 1),
    max_value=datetime(2025, 12, 31)
)

# Strategy for generating endpoint names
endpoint_strategy = st.sampled_from([
    'Property', 'Media', 'Member', 'Office', 'Listing'
])

# Strategy for generating record counts
record_count_strategy = st.integers(min_value=0, max_value=10000)

# Strategy for generating response times
response_time_strategy = st.floats(min_value=0.0, max_value=5000.0, allow_nan=False)

# Strategy for generating cost per call
cost_per_call_strategy = st.floats(min_value=0.0001, max_value=1.0, allow_nan=False)

# Strategy for generating quota values
quota_strategy = st.integers(min_value=0, max_value=10000)


class TestCostTrackingAccuracy:
    """
    Property 18: Cost Tracking Accuracy
    
    For any API usage, the cost monitor should accurately track and report 
    usage statistics including API calls and estimated costs.
    
    Validates: Requirements 6.4
    """
    
    @given(
        num_calls=st.integers(min_value=1, max_value=100),
        cost_per_call=cost_per_call_strategy
    )
    @settings(max_examples=100)
    def test_total_cost_equals_calls_times_cost_per_call(
        self, num_calls, cost_per_call
    ):
        """
        Feature: trestle-etl-pipeline, Property 18: Cost Tracking Accuracy
        
        For any number of API calls, the total estimated cost should equal
        the number of calls multiplied by the cost per call.
        """
        monitor = CostMonitor(cost_per_call=cost_per_call)
        
        # Track multiple API calls
        for i in range(num_calls):
            monitor.track_api_call(
                endpoint=f"Property",
                records_returned=10,
                response_time_ms=100.0,
                success=True
            )
        
        # Verify total cost calculation
        expected_cost = num_calls * cost_per_call
        actual_cost = monitor.get_total_estimated_cost()
        
        assert abs(actual_cost - expected_cost) < 0.0001, \
            f"Expected cost {expected_cost}, got {actual_cost}"
    
    @given(
        endpoints=st.lists(endpoint_strategy, min_size=1, max_size=50),
        records_per_call=st.lists(record_count_strategy, min_size=1, max_size=50)
    )
    @settings(max_examples=100)
    def test_total_records_fetched_is_sum_of_all_calls(
        self, endpoints, records_per_call
    ):
        """
        Feature: trestle-etl-pipeline, Property 18: Cost Tracking Accuracy
        
        For any series of API calls, the total records fetched should equal
        the sum of records from all calls.
        """
        monitor = CostMonitor()
        
        # Ensure same length
        min_len = min(len(endpoints), len(records_per_call))
        endpoints = endpoints[:min_len]
        records_per_call = records_per_call[:min_len]
        
        # Track API calls
        for endpoint, records in zip(endpoints, records_per_call):
            monitor.track_api_call(
                endpoint=endpoint,
                records_returned=records,
                success=True
            )
        
        # Verify total records
        expected_total = sum(records_per_call)
        actual_total = monitor.get_total_records_fetched()
        
        assert actual_total == expected_total, \
            f"Expected {expected_total} records, got {actual_total}"
    
    @given(
        num_successful=st.integers(min_value=0, max_value=50),
        num_failed=st.integers(min_value=0, max_value=50)
    )
    @settings(max_examples=100)
    def test_success_rate_calculation_is_accurate(
        self, num_successful, num_failed
    ):
        """
        Feature: trestle-etl-pipeline, Property 18: Cost Tracking Accuracy
        
        For any mix of successful and failed calls, the success rate should
        be accurately calculated.
        """
        assume(num_successful + num_failed > 0)
        
        monitor = CostMonitor()
        
        # Track successful calls
        for _ in range(num_successful):
            monitor.track_api_call(
                endpoint="Property",
                records_returned=10,
                success=True
            )
        
        # Track failed calls
        for _ in range(num_failed):
            monitor.track_api_call(
                endpoint="Property",
                records_returned=0,
                success=False,
                error_message="Test error"
            )
        
        # Generate report and verify success rate
        report = monitor.generate_usage_report()
        
        total_calls = num_successful + num_failed
        expected_rate = (num_successful / total_calls) * 100
        
        assert abs(report.success_rate - expected_rate) < 0.01, \
            f"Expected success rate {expected_rate}%, got {report.success_rate}%"
        
        assert report.successful_calls == num_successful
        assert report.failed_calls == num_failed
        assert report.total_api_calls == total_calls
    
    @given(
        response_times=st.lists(
            st.floats(min_value=0.1, max_value=5000.0, allow_nan=False),
            min_size=1,
            max_size=50
        )
    )
    @settings(max_examples=100)
    def test_average_response_time_calculation(self, response_times):
        """
        Feature: trestle-etl-pipeline, Property 18: Cost Tracking Accuracy
        
        For any series of API calls with positive response times, the average 
        response time should be accurately calculated.
        Note: Response times of 0.0 are filtered out as they indicate
        unrecorded/invalid response times.
        """
        monitor = CostMonitor()
        
        # Track API calls with response times
        for response_time in response_times:
            monitor.track_api_call(
                endpoint="Property",
                records_returned=10,
                response_time_ms=response_time,
                success=True
            )
        
        # Generate report
        report = monitor.generate_usage_report()
        
        # Calculate expected average (only positive response times)
        positive_times = [t for t in response_times if t > 0]
        expected_avg = sum(positive_times) / len(positive_times) if positive_times else 0.0
        
        assert abs(report.average_response_time_ms - expected_avg) < 0.01, \
            f"Expected avg {expected_avg}ms, got {report.average_response_time_ms}ms"
    
    @given(
        minute_limit=quota_strategy,
        minute_remaining=quota_strategy,
        hour_limit=quota_strategy,
        hour_remaining=quota_strategy
    )
    @settings(max_examples=100)
    def test_quota_status_tracking(
        self, minute_limit, minute_remaining, hour_limit, hour_remaining
    ):
        """
        Feature: trestle-etl-pipeline, Property 18: Cost Tracking Accuracy
        
        For any quota information, the quota status should be accurately
        tracked and reported.
        """
        monitor = CostMonitor()
        
        quota_info = {
            'minute_quota_limit': minute_limit,
            'minute_quota_remaining': minute_remaining,
            'hour_quota_limit': hour_limit,
            'hour_quota_remaining': hour_remaining
        }
        
        # Update quota status
        monitor.update_quota_status(quota_info)
        
        # Verify quota status
        status = monitor.get_quota_status()
        
        assert status.minute_limit == minute_limit
        assert status.minute_remaining == minute_remaining
        assert status.hour_limit == hour_limit
        assert status.hour_remaining == hour_remaining
        assert status.last_updated is not None
    
    @given(
        limit=st.integers(min_value=100, max_value=1000),
        remaining_percent=st.floats(min_value=0.0, max_value=1.0, allow_nan=False)
    )
    @settings(max_examples=100)
    def test_quota_usage_percentage_calculation(self, limit, remaining_percent):
        """
        Feature: trestle-etl-pipeline, Property 18: Cost Tracking Accuracy
        
        For any quota limit and remaining value, the usage percentage should
        be accurately calculated.
        """
        remaining = int(limit * remaining_percent)
        
        monitor = CostMonitor()
        
        quota_info = {
            'minute_quota_limit': limit,
            'minute_quota_remaining': remaining
        }
        
        monitor.update_quota_status(quota_info)
        status = monitor.get_quota_status()
        
        # Calculate expected usage percentage
        expected_usage = ((limit - remaining) / limit) * 100
        
        assert abs(status.minute_usage_percent - expected_usage) < 0.01, \
            f"Expected usage {expected_usage}%, got {status.minute_usage_percent}%"
    
    @given(
        num_calls=st.integers(min_value=1, max_value=50),
        cost_per_call=cost_per_call_strategy
    )
    @settings(max_examples=100)
    def test_usage_report_cost_matches_calculated_cost(
        self, num_calls, cost_per_call
    ):
        """
        Feature: trestle-etl-pipeline, Property 18: Cost Tracking Accuracy
        
        For any usage report, the estimated cost should match the calculated
        cost based on API calls.
        """
        monitor = CostMonitor(cost_per_call=cost_per_call)
        
        # Track API calls
        for _ in range(num_calls):
            monitor.track_api_call(
                endpoint="Property",
                records_returned=10,
                success=True
            )
        
        # Generate report
        report = monitor.generate_usage_report()
        
        # Verify cost in report
        expected_cost = num_calls * cost_per_call
        
        assert abs(report.estimated_cost - expected_cost) < 0.0001, \
            f"Expected cost {expected_cost}, got {report.estimated_cost}"
    
    @given(
        warning_threshold=st.floats(min_value=0.5, max_value=0.9, allow_nan=False),
        critical_threshold=st.floats(min_value=0.8, max_value=0.99, allow_nan=False)
    )
    @settings(max_examples=50)
    def test_quota_alerts_triggered_at_thresholds(
        self, warning_threshold, critical_threshold
    ):
        """
        Feature: trestle-etl-pipeline, Property 18: Cost Tracking Accuracy
        
        For any threshold configuration, alerts should be triggered when
        quota usage exceeds the thresholds.
        """
        # Ensure critical > warning
        if critical_threshold <= warning_threshold:
            critical_threshold = warning_threshold + 0.05
        
        alerts_received = []
        
        def alert_callback(level, message):
            alerts_received.append((level, message))
        
        monitor = CostMonitor(
            warning_threshold=warning_threshold,
            critical_threshold=critical_threshold,
            alert_callback=alert_callback
        )
        
        # Set quota at critical level
        limit = 1000
        remaining = int(limit * (1 - critical_threshold - 0.01))  # Just past critical
        
        quota_info = {
            'minute_quota_limit': limit,
            'minute_quota_remaining': remaining
        }
        
        monitor.track_api_call(
            endpoint="Property",
            records_returned=10,
            quota_info=quota_info
        )
        
        # Verify critical alert was triggered
        critical_alerts = [a for a in alerts_received if a[0] == AlertLevel.CRITICAL]
        assert len(critical_alerts) > 0, "Critical alert should be triggered"
    
    @given(num_calls=st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_api_call_count_tracking(self, num_calls):
        """
        Feature: trestle-etl-pipeline, Property 18: Cost Tracking Accuracy
        
        For any number of API calls, the total count should be accurately
        tracked.
        """
        monitor = CostMonitor()
        
        # Track API calls
        for _ in range(num_calls):
            monitor.track_api_call(
                endpoint="Property",
                records_returned=10,
                success=True
            )
        
        # Verify count
        assert monitor.get_total_api_calls() == num_calls
    
    @given(
        limit=st.integers(min_value=100, max_value=1000),
        threshold=st.floats(min_value=0.05, max_value=0.5, allow_nan=False)
    )
    @settings(max_examples=100)
    def test_check_quota_limits_detects_low_quota(self, limit, threshold):
        """
        Feature: trestle-etl-pipeline, Property 18: Cost Tracking Accuracy
        
        For any quota limit and threshold, the system should correctly detect
        when quota is approaching limits.
        """
        monitor = CostMonitor()
        
        # Set quota just below threshold
        remaining = int(limit * (threshold - 0.01))
        
        quota_info = {
            'minute_quota_limit': limit,
            'minute_quota_remaining': remaining
        }
        
        monitor.update_quota_status(quota_info)
        
        # Check quota limits
        alerts = monitor.check_quota_limits(threshold_percent=threshold)
        
        assert alerts.get('minute_quota_low', False), \
            f"Should detect low quota: {remaining}/{limit} remaining, threshold={threshold}"
    
    @given(
        limit=st.integers(min_value=100, max_value=1000)
    )
    @settings(max_examples=50)
    def test_should_pause_operations_when_quota_exhausted(self, limit):
        """
        Feature: trestle-etl-pipeline, Property 18: Cost Tracking Accuracy
        
        For any exhausted quota, the system should indicate operations
        should be paused.
        """
        monitor = CostMonitor()
        
        # Set quota to exhausted
        quota_info = {
            'minute_quota_limit': limit,
            'minute_quota_remaining': 0
        }
        
        monitor.update_quota_status(quota_info)
        
        # Verify pause recommendation
        assert monitor.should_pause_operations(), \
            "Should recommend pausing when quota is exhausted"
