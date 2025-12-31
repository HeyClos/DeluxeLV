"""
Cost Monitor for Trestle ETL Pipeline.

Handles API usage tracking, cost estimation, and usage reporting/alerting.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum


class CostMonitorError(Exception):
    """Raised when cost monitoring operations fail."""
    pass


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class APIUsageRecord:
    """Record of a single API usage event."""
    timestamp: datetime
    endpoint: str
    records_returned: int
    quota_used: int = 1
    response_time_ms: float = 0.0
    success: bool = True
    error_message: Optional[str] = None


@dataclass
class QuotaStatus:
    """Current quota status."""
    minute_limit: int = 0
    minute_remaining: int = 0
    hour_limit: int = 0
    hour_remaining: int = 0
    daily_limit: int = 0
    daily_remaining: int = 0
    last_updated: Optional[datetime] = None
    
    @property
    def minute_usage_percent(self) -> float:
        """Calculate minute quota usage percentage."""
        if self.minute_limit <= 0:
            return 0.0
        return ((self.minute_limit - self.minute_remaining) / self.minute_limit) * 100
    
    @property
    def hour_usage_percent(self) -> float:
        """Calculate hour quota usage percentage."""
        if self.hour_limit <= 0:
            return 0.0
        return ((self.hour_limit - self.hour_remaining) / self.hour_limit) * 100
    
    @property
    def daily_usage_percent(self) -> float:
        """Calculate daily quota usage percentage."""
        if self.daily_limit <= 0:
            return 0.0
        return ((self.daily_limit - self.daily_remaining) / self.daily_limit) * 100


@dataclass
class UsageReport:
    """Summary report of API usage."""
    period_start: datetime
    period_end: datetime
    total_api_calls: int = 0
    total_records_fetched: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    average_response_time_ms: float = 0.0
    estimated_cost: float = 0.0
    quota_status: Optional[QuotaStatus] = None
    alerts_triggered: List[str] = field(default_factory=list)
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_api_calls <= 0:
            return 100.0
        return (self.successful_calls / self.total_api_calls) * 100


@dataclass
class CostEstimate:
    """Cost estimation for API usage."""
    api_calls: int
    estimated_cost_usd: float
    cost_per_call: float
    period: str  # e.g., "daily", "monthly"


class CostMonitor:
    """
    Monitors API usage and associated costs for the Trestle ETL pipeline.
    
    Handles:
    - Tracking API calls and quota usage
    - Cost estimation based on usage patterns
    - Usage reporting and alerting
    - Quota limit monitoring
    """
    
    # Default cost per API call (configurable)
    DEFAULT_COST_PER_CALL = 0.001  # $0.001 per call
    
    # Alert thresholds (percentage of quota used)
    DEFAULT_WARNING_THRESHOLD = 0.75  # 75%
    DEFAULT_CRITICAL_THRESHOLD = 0.90  # 90%
    
    def __init__(
        self,
        cost_per_call: float = DEFAULT_COST_PER_CALL,
        warning_threshold: float = DEFAULT_WARNING_THRESHOLD,
        critical_threshold: float = DEFAULT_CRITICAL_THRESHOLD,
        alert_callback: Optional[Callable[[AlertLevel, str], None]] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize CostMonitor.
        
        Args:
            cost_per_call: Estimated cost per API call in USD.
            warning_threshold: Quota usage threshold for warnings (0-1).
            critical_threshold: Quota usage threshold for critical alerts (0-1).
            alert_callback: Optional callback for alert notifications.
            logger: Optional logger instance.
        """
        self.cost_per_call = cost_per_call
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self.alert_callback = alert_callback
        self.logger = logger or logging.getLogger(__name__)
        
        self._usage_records: List[APIUsageRecord] = []
        self._quota_status = QuotaStatus()
        self._alerts_triggered: List[Dict[str, Any]] = []
        self._session_start = datetime.now()
    
    def track_api_call(
        self,
        endpoint: str,
        records_returned: int,
        response_time_ms: float = 0.0,
        success: bool = True,
        error_message: Optional[str] = None,
        quota_info: Optional[Dict[str, Any]] = None
    ) -> APIUsageRecord:
        """
        Record an API call for tracking.
        
        Args:
            endpoint: API endpoint called.
            records_returned: Number of records returned.
            response_time_ms: Response time in milliseconds.
            success: Whether the call was successful.
            error_message: Error message if call failed.
            quota_info: Quota information from response headers.
            
        Returns:
            APIUsageRecord for the tracked call.
        """
        record = APIUsageRecord(
            timestamp=datetime.now(),
            endpoint=endpoint,
            records_returned=records_returned,
            response_time_ms=response_time_ms,
            success=success,
            error_message=error_message
        )
        
        self._usage_records.append(record)
        
        # Update quota status if provided
        if quota_info:
            self.update_quota_status(quota_info)
        
        # Check for quota alerts
        self._check_quota_alerts()
        
        self.logger.debug(
            f"Tracked API call: {endpoint}, "
            f"records={records_returned}, "
            f"success={success}, "
            f"time={response_time_ms:.2f}ms"
        )
        
        return record
    
    def update_quota_status(self, quota_info: Dict[str, Any]) -> QuotaStatus:
        """
        Update quota status from API response headers.
        
        Args:
            quota_info: Dictionary with quota information.
            
        Returns:
            Updated QuotaStatus.
        """
        self._quota_status.minute_limit = quota_info.get('minute_quota_limit', 0)
        self._quota_status.minute_remaining = quota_info.get('minute_quota_remaining', 0)
        self._quota_status.hour_limit = quota_info.get('hour_quota_limit', 0)
        self._quota_status.hour_remaining = quota_info.get('hour_quota_remaining', 0)
        self._quota_status.daily_limit = quota_info.get('daily_quota_limit', 0)
        self._quota_status.daily_remaining = quota_info.get('daily_quota_remaining', 0)
        self._quota_status.last_updated = datetime.now()
        
        return self._quota_status
    
    def get_quota_status(self) -> QuotaStatus:
        """Get current quota status."""
        return self._quota_status
    
    def calculate_costs(
        self,
        period_start: Optional[datetime] = None,
        period_end: Optional[datetime] = None
    ) -> CostEstimate:
        """
        Calculate estimated costs for a time period.
        
        Args:
            period_start: Start of period (default: session start).
            period_end: End of period (default: now).
            
        Returns:
            CostEstimate with calculated costs.
        """
        period_start = period_start or self._session_start
        period_end = period_end or datetime.now()
        
        # Filter records within period
        period_records = [
            r for r in self._usage_records
            if period_start <= r.timestamp <= period_end
        ]
        
        api_calls = len(period_records)
        estimated_cost = api_calls * self.cost_per_call
        
        # Determine period label
        duration = period_end - period_start
        if duration.days >= 30:
            period = "monthly"
        elif duration.days >= 7:
            period = "weekly"
        elif duration.days >= 1:
            period = "daily"
        else:
            period = "session"
        
        return CostEstimate(
            api_calls=api_calls,
            estimated_cost_usd=estimated_cost,
            cost_per_call=self.cost_per_call,
            period=period
        )

    def check_quota_limits(self, threshold_percent: float = 0.1) -> Dict[str, bool]:
        """
        Check if quota usage is approaching limits.
        
        Args:
            threshold_percent: Threshold as percentage remaining (0.1 = 10%).
            
        Returns:
            Dictionary indicating which quotas are approaching limits.
        """
        alerts = {}
        
        # Check minute quota
        if self._quota_status.minute_limit > 0:
            remaining_percent = self._quota_status.minute_remaining / self._quota_status.minute_limit
            alerts['minute_quota_low'] = remaining_percent <= threshold_percent
        
        # Check hour quota
        if self._quota_status.hour_limit > 0:
            remaining_percent = self._quota_status.hour_remaining / self._quota_status.hour_limit
            alerts['hour_quota_low'] = remaining_percent <= threshold_percent
        
        # Check daily quota
        if self._quota_status.daily_limit > 0:
            remaining_percent = self._quota_status.daily_remaining / self._quota_status.daily_limit
            alerts['daily_quota_low'] = remaining_percent <= threshold_percent
        
        return alerts
    
    def _check_quota_alerts(self) -> None:
        """Check quota status and trigger alerts if needed."""
        quota = self._quota_status
        
        # Check minute quota
        if quota.minute_limit > 0:
            usage_percent = quota.minute_usage_percent / 100
            if usage_percent >= self.critical_threshold:
                self._trigger_alert(
                    AlertLevel.CRITICAL,
                    f"Minute quota critical: {quota.minute_usage_percent:.1f}% used "
                    f"({quota.minute_remaining}/{quota.minute_limit} remaining)"
                )
            elif usage_percent >= self.warning_threshold:
                self._trigger_alert(
                    AlertLevel.WARNING,
                    f"Minute quota warning: {quota.minute_usage_percent:.1f}% used "
                    f"({quota.minute_remaining}/{quota.minute_limit} remaining)"
                )
        
        # Check hour quota
        if quota.hour_limit > 0:
            usage_percent = quota.hour_usage_percent / 100
            if usage_percent >= self.critical_threshold:
                self._trigger_alert(
                    AlertLevel.CRITICAL,
                    f"Hour quota critical: {quota.hour_usage_percent:.1f}% used "
                    f"({quota.hour_remaining}/{quota.hour_limit} remaining)"
                )
            elif usage_percent >= self.warning_threshold:
                self._trigger_alert(
                    AlertLevel.WARNING,
                    f"Hour quota warning: {quota.hour_usage_percent:.1f}% used "
                    f"({quota.hour_remaining}/{quota.hour_limit} remaining)"
                )
        
        # Check daily quota
        if quota.daily_limit > 0:
            usage_percent = quota.daily_usage_percent / 100
            if usage_percent >= self.critical_threshold:
                self._trigger_alert(
                    AlertLevel.CRITICAL,
                    f"Daily quota critical: {quota.daily_usage_percent:.1f}% used "
                    f"({quota.daily_remaining}/{quota.daily_limit} remaining)"
                )
            elif usage_percent >= self.warning_threshold:
                self._trigger_alert(
                    AlertLevel.WARNING,
                    f"Daily quota warning: {quota.daily_usage_percent:.1f}% used "
                    f"({quota.daily_remaining}/{quota.daily_limit} remaining)"
                )
    
    def _trigger_alert(self, level: AlertLevel, message: str) -> None:
        """
        Trigger an alert.
        
        Args:
            level: Alert severity level.
            message: Alert message.
        """
        alert = {
            'timestamp': datetime.now(),
            'level': level,
            'message': message
        }
        self._alerts_triggered.append(alert)
        
        # Log the alert
        if level == AlertLevel.CRITICAL:
            self.logger.critical(message)
        elif level == AlertLevel.WARNING:
            self.logger.warning(message)
        else:
            self.logger.info(message)
        
        # Call alert callback if provided
        if self.alert_callback:
            try:
                self.alert_callback(level, message)
            except Exception as e:
                self.logger.error(f"Alert callback failed: {e}")
    
    def generate_usage_report(
        self,
        period_start: Optional[datetime] = None,
        period_end: Optional[datetime] = None
    ) -> UsageReport:
        """
        Generate a usage report for a time period.
        
        Args:
            period_start: Start of period (default: session start).
            period_end: End of period (default: now).
            
        Returns:
            UsageReport with usage statistics.
        """
        period_start = period_start or self._session_start
        period_end = period_end or datetime.now()
        
        # Filter records within period
        period_records = [
            r for r in self._usage_records
            if period_start <= r.timestamp <= period_end
        ]
        
        total_calls = len(period_records)
        successful_calls = sum(1 for r in period_records if r.success)
        failed_calls = total_calls - successful_calls
        total_records = sum(r.records_returned for r in period_records)
        
        # Calculate average response time
        response_times = [r.response_time_ms for r in period_records if r.response_time_ms > 0]
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0.0
        
        # Calculate costs
        cost_estimate = self.calculate_costs(period_start, period_end)
        
        # Get alerts for period
        period_alerts = [
            a['message'] for a in self._alerts_triggered
            if period_start <= a['timestamp'] <= period_end
        ]
        
        return UsageReport(
            period_start=period_start,
            period_end=period_end,
            total_api_calls=total_calls,
            total_records_fetched=total_records,
            successful_calls=successful_calls,
            failed_calls=failed_calls,
            average_response_time_ms=avg_response_time,
            estimated_cost=cost_estimate.estimated_cost_usd,
            quota_status=self._quota_status,
            alerts_triggered=period_alerts
        )
    
    def should_pause_operations(self) -> bool:
        """
        Determine if operations should be paused due to quota limits.
        
        Returns:
            True if operations should be paused.
        """
        quota = self._quota_status
        
        # Pause if any quota is exhausted
        if quota.minute_limit > 0 and quota.minute_remaining <= 0:
            return True
        if quota.hour_limit > 0 and quota.hour_remaining <= 0:
            return True
        if quota.daily_limit > 0 and quota.daily_remaining <= 0:
            return True
        
        return False
    
    def get_usage_records(
        self,
        period_start: Optional[datetime] = None,
        period_end: Optional[datetime] = None
    ) -> List[APIUsageRecord]:
        """
        Get usage records for a time period.
        
        Args:
            period_start: Start of period (default: all records).
            period_end: End of period (default: all records).
            
        Returns:
            List of APIUsageRecord objects.
        """
        if period_start is None and period_end is None:
            return self._usage_records.copy()
        
        period_start = period_start or datetime.min
        period_end = period_end or datetime.max
        
        return [
            r for r in self._usage_records
            if period_start <= r.timestamp <= period_end
        ]
    
    def get_alerts(
        self,
        level: Optional[AlertLevel] = None,
        period_start: Optional[datetime] = None,
        period_end: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get triggered alerts.
        
        Args:
            level: Filter by alert level (optional).
            period_start: Start of period (optional).
            period_end: End of period (optional).
            
        Returns:
            List of alert dictionaries.
        """
        alerts = self._alerts_triggered.copy()
        
        if level:
            alerts = [a for a in alerts if a['level'] == level]
        
        if period_start:
            alerts = [a for a in alerts if a['timestamp'] >= period_start]
        
        if period_end:
            alerts = [a for a in alerts if a['timestamp'] <= period_end]
        
        return alerts
    
    def clear_usage_records(self) -> None:
        """Clear all usage records."""
        self._usage_records.clear()
        self._session_start = datetime.now()
    
    def clear_alerts(self) -> None:
        """Clear all triggered alerts."""
        self._alerts_triggered.clear()
    
    def get_total_api_calls(self) -> int:
        """Get total number of API calls tracked."""
        return len(self._usage_records)
    
    def get_total_records_fetched(self) -> int:
        """Get total number of records fetched."""
        return sum(r.records_returned for r in self._usage_records)
    
    def get_total_estimated_cost(self) -> float:
        """Get total estimated cost in USD."""
        return len(self._usage_records) * self.cost_per_call
