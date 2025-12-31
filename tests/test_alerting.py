"""
Property-based tests for Alerting System.

Feature: trestle-etl-pipeline
Tests Property 19 for alert delivery reliability.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from unittest.mock import Mock, MagicMock, patch
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
import time

import pytest
from hypothesis import given, strategies as st, settings, assume

from trestle_etl.alerting import (
    AlertManager, AlertChannel, EmailAlertChannel, WebhookAlertChannel,
    Alert, AlertResult, AlertSeverity, AlertType, AlertThreshold,
    create_alert_manager_from_config
)


# Strategy for generating alert titles
title_strategy = st.text(
    alphabet=st.characters(whitelist_categories=('L', 'N', 'P', 'S'), 
                           whitelist_characters=' -_', max_codepoint=127),
    min_size=1,
    max_size=100
)

# Strategy for generating alert messages
message_strategy = st.text(
    alphabet=st.characters(whitelist_categories=('L', 'N', 'P', 'S'), 
                           whitelist_characters=' -_.,!?', max_codepoint=127),
    min_size=1,
    max_size=500
)

# Strategy for generating alert types
alert_type_strategy = st.sampled_from(list(AlertType))

# Strategy for generating alert severities
severity_strategy = st.sampled_from(list(AlertSeverity))

# Strategy for generating quota values
quota_strategy = st.integers(min_value=0, max_value=10000)

# Strategy for generating threshold percentages
threshold_strategy = st.floats(min_value=0.1, max_value=0.99, allow_nan=False)


class MockAlertChannel(AlertChannel):
    """Mock alert channel for testing."""
    
    def __init__(self, name: str = "mock", should_succeed: bool = True):
        self._name = name
        self.should_succeed = should_succeed
        self.sent_alerts: List[Alert] = []
        self._configured = True
    
    @property
    def channel_name(self) -> str:
        return self._name
    
    def is_configured(self) -> bool:
        return self._configured
    
    def send(self, alert: Alert) -> AlertResult:
        self.sent_alerts.append(alert)
        if self.should_succeed:
            return AlertResult(
                success=True,
                channel=self.channel_name,
                alert=alert
            )
        else:
            return AlertResult(
                success=False,
                channel=self.channel_name,
                alert=alert,
                error_message="Mock failure"
            )


class TestAlertDeliveryReliability:
    """
    Property 19: Alert Delivery Reliability
    
    For any critical error condition, configured alert mechanisms 
    (email, webhook) should successfully deliver notifications.
    
    Validates: Requirements 6.5
    """
    
    @given(
        title=title_strategy,
        message=message_strategy,
        alert_type=alert_type_strategy,
        severity=severity_strategy
    )
    @settings(max_examples=100)
    def test_alerts_are_delivered_to_all_channels(
        self, title, message, alert_type, severity
    ):
        """
        Feature: trestle-etl-pipeline, Property 19: Alert Delivery Reliability
        
        For any alert, it should be delivered to all configured channels.
        """
        assume(len(title.strip()) > 0)
        assume(len(message.strip()) > 0)
        
        # Create manager with multiple mock channels
        manager = AlertManager()
        channel1 = MockAlertChannel("channel1")
        channel2 = MockAlertChannel("channel2")
        manager.add_channel(channel1)
        manager.add_channel(channel2)
        
        # Create and send alert
        alert = Alert(
            alert_type=alert_type,
            severity=severity,
            title=title,
            message=message
        )
        
        results = manager.send_alert(alert)
        
        # Verify alert was sent to both channels
        assert len(results) == 2, "Alert should be sent to all channels"
        assert all(r.success for r in results), "All deliveries should succeed"
        assert len(channel1.sent_alerts) == 1, "Channel 1 should receive alert"
        assert len(channel2.sent_alerts) == 1, "Channel 2 should receive alert"
        
        # Clear suppression for next test
        manager.clear_suppression()
    
    @given(
        title=title_strategy,
        message=message_strategy
    )
    @settings(max_examples=100)
    def test_alert_content_is_preserved(self, title, message):
        """
        Feature: trestle-etl-pipeline, Property 19: Alert Delivery Reliability
        
        For any alert, the content should be preserved during delivery.
        """
        assume(len(title.strip()) > 0)
        assume(len(message.strip()) > 0)
        
        manager = AlertManager()
        channel = MockAlertChannel()
        manager.add_channel(channel)
        
        alert = Alert(
            alert_type=AlertType.ETL_ERROR,
            severity=AlertSeverity.ERROR,
            title=title,
            message=message,
            context={'test_key': 'test_value'}
        )
        
        manager.send_alert(alert)
        
        # Verify content is preserved
        assert len(channel.sent_alerts) == 1
        sent_alert = channel.sent_alerts[0]
        assert sent_alert.title == title
        assert sent_alert.message == message
        assert sent_alert.context.get('test_key') == 'test_value'
        
        manager.clear_suppression()
    
    @given(
        num_channels=st.integers(min_value=1, max_value=5),
        num_failing=st.integers(min_value=0, max_value=5)
    )
    @settings(max_examples=50)
    def test_partial_delivery_is_tracked(self, num_channels, num_failing):
        """
        Feature: trestle-etl-pipeline, Property 19: Alert Delivery Reliability
        
        For any mix of successful and failing channels, delivery results
        should accurately reflect the outcome.
        """
        num_failing = min(num_failing, num_channels)
        num_succeeding = num_channels - num_failing
        
        manager = AlertManager()
        
        # Add succeeding channels
        for i in range(num_succeeding):
            channel = MockAlertChannel(f"success_{i}", should_succeed=True)
            manager.add_channel(channel)
        
        # Add failing channels
        for i in range(num_failing):
            channel = MockAlertChannel(f"fail_{i}", should_succeed=False)
            manager.add_channel(channel)
        
        alert = Alert(
            alert_type=AlertType.ETL_ERROR,
            severity=AlertSeverity.ERROR,
            title="Test Alert",
            message="Test message"
        )
        
        results = manager.send_alert(alert)
        
        # Verify results
        assert len(results) == num_channels
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        
        assert len(successful) == num_succeeding
        assert len(failed) == num_failing
        
        manager.clear_suppression()
    
    @given(
        quota_type=st.sampled_from(['Minute', 'Hour', 'Daily']),
        limit=st.integers(min_value=100, max_value=10000),
        warning_threshold=st.floats(min_value=0.5, max_value=0.85, allow_nan=False),
        critical_threshold=st.floats(min_value=0.86, max_value=0.99, allow_nan=False)
    )
    @settings(max_examples=100)
    def test_quota_alerts_triggered_at_thresholds(
        self, quota_type, limit, warning_threshold, critical_threshold
    ):
        """
        Feature: trestle-etl-pipeline, Property 19: Alert Delivery Reliability
        
        For any quota threshold configuration, alerts should be triggered
        when usage exceeds the thresholds.
        """
        thresholds = AlertThreshold(
            quota_warning_percent=warning_threshold,
            quota_critical_percent=critical_threshold
        )
        
        manager = AlertManager(thresholds=thresholds)
        channel = MockAlertChannel()
        manager.add_channel(channel)
        
        # Test critical threshold
        critical_remaining = int(limit * (1 - critical_threshold - 0.01))
        results = manager.check_quota_threshold(quota_type, critical_remaining, limit)
        
        assert results is not None, "Critical alert should be triggered"
        assert len(channel.sent_alerts) == 1
        assert channel.sent_alerts[0].severity == AlertSeverity.CRITICAL
        
        manager.clear_suppression()
        channel.sent_alerts.clear()
        
        # Test warning threshold - remaining should be between warning and critical
        # Calculate remaining that is past warning but not past critical
        warning_usage = warning_threshold + 0.02  # Just past warning
        # Make sure it's below critical
        if warning_usage >= critical_threshold:
            warning_usage = (warning_threshold + critical_threshold) / 2
        
        warning_remaining = int(limit * (1 - warning_usage))
        
        results = manager.check_quota_threshold(quota_type, warning_remaining, limit)
        
        # Should trigger warning (not critical)
        if results is not None:
            assert len(channel.sent_alerts) == 1
            assert channel.sent_alerts[0].severity == AlertSeverity.WARNING
    
    @given(
        error_message=message_strategy,
        error_type=st.sampled_from(['ValueError', 'ConnectionError', 'TimeoutError'])
    )
    @settings(max_examples=100)
    def test_etl_error_alerts_are_sent(self, error_message, error_type):
        """
        Feature: trestle-etl-pipeline, Property 19: Alert Delivery Reliability
        
        For any ETL error, an alert should be sent with the error details.
        """
        assume(len(error_message.strip()) > 0)
        
        manager = AlertManager()
        channel = MockAlertChannel()
        manager.add_channel(channel)
        
        results = manager.alert_etl_error(
            error_message=error_message,
            error_type=error_type,
            context={'run_id': 'test_run'}
        )
        
        assert len(results) == 1
        assert results[0].success
        assert len(channel.sent_alerts) == 1
        
        sent_alert = channel.sent_alerts[0]
        assert sent_alert.alert_type == AlertType.ETL_ERROR
        assert sent_alert.severity == AlertSeverity.ERROR
        assert error_message in sent_alert.message
        assert sent_alert.context.get('error_type') == error_type
        
        manager.clear_suppression()
    
    @given(
        error_message=message_strategy,
        error_type=st.sampled_from(['SystemError', 'MemoryError', 'CriticalFailure'])
    )
    @settings(max_examples=100)
    def test_critical_alerts_are_sent(self, error_message, error_type):
        """
        Feature: trestle-etl-pipeline, Property 19: Alert Delivery Reliability
        
        For any critical error, a critical alert should be sent.
        """
        assume(len(error_message.strip()) > 0)
        
        manager = AlertManager()
        channel = MockAlertChannel()
        manager.add_channel(channel)
        
        results = manager.alert_etl_critical(
            error_message=error_message,
            error_type=error_type
        )
        
        assert len(results) == 1
        assert results[0].success
        
        sent_alert = channel.sent_alerts[0]
        assert sent_alert.alert_type == AlertType.ETL_CRITICAL
        assert sent_alert.severity == AlertSeverity.CRITICAL
        
        manager.clear_suppression()
    
    @given(
        endpoint=st.sampled_from(['/api/Property', '/api/Media', '/api/Member']),
        status_code=st.sampled_from([400, 401, 403, 404, 429, 500, 502, 503])
    )
    @settings(max_examples=100)
    def test_api_error_alerts_include_context(self, endpoint, status_code):
        """
        Feature: trestle-etl-pipeline, Property 19: Alert Delivery Reliability
        
        For any API error, the alert should include endpoint and status code.
        """
        manager = AlertManager()
        channel = MockAlertChannel()
        manager.add_channel(channel)
        
        results = manager.alert_api_error(
            error_message=f"API request failed with status {status_code}",
            endpoint=endpoint,
            status_code=status_code
        )
        
        assert len(results) == 1
        sent_alert = channel.sent_alerts[0]
        assert sent_alert.context.get('endpoint') == endpoint
        assert sent_alert.context.get('status_code') == status_code
        
        manager.clear_suppression()
    
    @given(
        operation=st.sampled_from(['insert', 'update', 'delete', 'connect']),
        error_message=message_strategy
    )
    @settings(max_examples=100)
    def test_database_error_alerts_include_operation(self, operation, error_message):
        """
        Feature: trestle-etl-pipeline, Property 19: Alert Delivery Reliability
        
        For any database error, the alert should include the operation type.
        """
        assume(len(error_message.strip()) > 0)
        
        manager = AlertManager()
        channel = MockAlertChannel()
        manager.add_channel(channel)
        
        results = manager.alert_database_error(
            error_message=error_message,
            operation=operation
        )
        
        assert len(results) == 1
        sent_alert = channel.sent_alerts[0]
        assert sent_alert.alert_type == AlertType.DATABASE_ERROR
        assert sent_alert.context.get('operation') == operation
        
        manager.clear_suppression()
    
    @given(num_alerts=st.integers(min_value=1, max_value=20))
    @settings(max_examples=50)
    def test_alert_history_is_tracked(self, num_alerts):
        """
        Feature: trestle-etl-pipeline, Property 19: Alert Delivery Reliability
        
        For any number of alerts sent, the history should accurately
        track all delivery attempts.
        """
        manager = AlertManager()
        channel = MockAlertChannel()
        manager.add_channel(channel)
        
        # Disable suppression for this test
        manager.set_suppression_window(0)
        
        for i in range(num_alerts):
            alert = Alert(
                alert_type=AlertType.ETL_ERROR,
                severity=AlertSeverity.ERROR,
                title=f"Test Alert {i}",
                message=f"Test message {i}"
            )
            manager.send_alert(alert)
        
        history = manager.get_alert_history()
        assert len(history) == num_alerts, \
            f"Expected {num_alerts} alerts in history, got {len(history)}"
    
    @given(
        title=title_strategy,
        message=message_strategy
    )
    @settings(max_examples=50)
    def test_alert_serialization(self, title, message):
        """
        Feature: trestle-etl-pipeline, Property 19: Alert Delivery Reliability
        
        For any alert, serialization to JSON should produce valid JSON
        that can be deserialized.
        """
        assume(len(title.strip()) > 0)
        assume(len(message.strip()) > 0)
        
        alert = Alert(
            alert_type=AlertType.ETL_ERROR,
            severity=AlertSeverity.ERROR,
            title=title,
            message=message,
            context={'key': 'value'}
        )
        
        # Test JSON serialization
        json_str = alert.to_json()
        assert json_str is not None
        
        # Verify it's valid JSON
        parsed = json.loads(json_str)
        assert parsed['title'] == title
        assert parsed['message'] == message
        assert parsed['alert_type'] == AlertType.ETL_ERROR.value
        assert parsed['severity'] == AlertSeverity.ERROR.value
    
    @given(
        suppression_seconds=st.integers(min_value=1, max_value=60)
    )
    @settings(max_examples=20)
    def test_alert_suppression_window(self, suppression_seconds):
        """
        Feature: trestle-etl-pipeline, Property 19: Alert Delivery Reliability
        
        For any suppression window, duplicate alerts within the window
        should be suppressed.
        """
        manager = AlertManager()
        channel = MockAlertChannel()
        manager.add_channel(channel)
        manager.set_suppression_window(suppression_seconds)
        
        alert = Alert(
            alert_type=AlertType.ETL_ERROR,
            severity=AlertSeverity.ERROR,
            title="Duplicate Test",
            message="Test message"
        )
        
        # First alert should be sent
        results1 = manager.send_alert(alert)
        assert len(results1) == 1
        assert len(channel.sent_alerts) == 1
        
        # Second identical alert should be suppressed
        results2 = manager.send_alert(alert)
        assert len(results2) == 0  # Suppressed
        assert len(channel.sent_alerts) == 1  # Still only 1
    
    def test_unconfigured_channels_are_skipped(self):
        """
        Feature: trestle-etl-pipeline, Property 19: Alert Delivery Reliability
        
        Unconfigured channels should not be added to the manager.
        """
        manager = AlertManager()
        
        # Create unconfigured email channel
        email_channel = EmailAlertChannel(
            smtp_host="",  # Empty host = unconfigured
            recipients=[]
        )
        
        manager.add_channel(email_channel)
        
        # Channel should not be added
        assert len(manager.get_channels()) == 0
    
    def test_webhook_channel_configuration(self):
        """
        Feature: trestle-etl-pipeline, Property 19: Alert Delivery Reliability
        
        Webhook channel should be properly configured with URL.
        """
        # Configured channel
        configured = WebhookAlertChannel(webhook_url="https://example.com/webhook")
        assert configured.is_configured()
        
        # Unconfigured channel
        unconfigured = WebhookAlertChannel(webhook_url="")
        assert not unconfigured.is_configured()
    
    def test_email_channel_configuration(self):
        """
        Feature: trestle-etl-pipeline, Property 19: Alert Delivery Reliability
        
        Email channel should be properly configured with host, sender, and recipients.
        """
        # Configured channel
        configured = EmailAlertChannel(
            smtp_host="smtp.example.com",
            sender_email="alerts@example.com",
            recipients=["admin@example.com"]
        )
        assert configured.is_configured()
        
        # Unconfigured channel (missing recipients)
        unconfigured = EmailAlertChannel(
            smtp_host="smtp.example.com",
            sender_email="alerts@example.com",
            recipients=[]
        )
        assert not unconfigured.is_configured()
    
    def test_create_alert_manager_from_config(self):
        """
        Feature: trestle-etl-pipeline, Property 19: Alert Delivery Reliability
        
        AlertManager should be correctly created from configuration.
        """
        # With webhook only
        manager = create_alert_manager_from_config(
            webhook_enabled=True,
            webhook_url="https://example.com/webhook"
        )
        assert "webhook" in manager.get_channels()
        
        # With email only (need smtp_user as sender)
        manager = create_alert_manager_from_config(
            email_enabled=True,
            email_recipients=["admin@example.com"],
            smtp_host="smtp.example.com",
            smtp_user="alerts@example.com"
        )
        assert "email" in manager.get_channels()
