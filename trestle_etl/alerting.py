"""
Alerting System for Trestle ETL Pipeline.

Provides email and webhook notification support for critical errors
and quota limit alerts.

Requirements: 6.5
"""

import json
import logging
import smtplib
import ssl
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Optional, Dict, Any, List, Callable
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertType(Enum):
    """Types of alerts."""
    QUOTA_WARNING = "quota_warning"
    QUOTA_CRITICAL = "quota_critical"
    ETL_ERROR = "etl_error"
    ETL_CRITICAL = "etl_critical"
    DATABASE_ERROR = "database_error"
    API_ERROR = "api_error"
    SYSTEM_ERROR = "system_error"


@dataclass
class Alert:
    """Represents an alert to be sent."""
    alert_type: AlertType
    severity: AlertSeverity
    title: str
    message: str
    timestamp: datetime = field(default_factory=datetime.now)
    context: Dict[str, Any] = field(default_factory=dict)
    source: str = "trestle_etl"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary."""
        return {
            'alert_type': self.alert_type.value,
            'severity': self.severity.value,
            'title': self.title,
            'message': self.message,
            'timestamp': self.timestamp.isoformat(),
            'context': self.context,
            'source': self.source
        }
    
    def to_json(self) -> str:
        """Convert alert to JSON string."""
        return json.dumps(self.to_dict())


@dataclass
class AlertResult:
    """Result of an alert delivery attempt."""
    success: bool
    channel: str
    alert: Alert
    error_message: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


class AlertChannel(ABC):
    """Abstract base class for alert delivery channels."""
    
    @property
    @abstractmethod
    def channel_name(self) -> str:
        """Return the name of this channel."""
        pass
    
    @abstractmethod
    def send(self, alert: Alert) -> AlertResult:
        """Send an alert through this channel."""
        pass
    
    @abstractmethod
    def is_configured(self) -> bool:
        """Check if this channel is properly configured."""
        pass


class EmailAlertChannel(AlertChannel):
    """Email alert delivery channel using SMTP."""
    
    def __init__(
        self,
        smtp_host: str,
        smtp_port: int = 587,
        smtp_user: Optional[str] = None,
        smtp_password: Optional[str] = None,
        sender_email: Optional[str] = None,
        recipients: Optional[List[str]] = None,
        use_tls: bool = True,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize email alert channel.
        
        Args:
            smtp_host: SMTP server hostname.
            smtp_port: SMTP server port.
            smtp_user: SMTP authentication username.
            smtp_password: SMTP authentication password.
            sender_email: Email address to send from.
            recipients: List of recipient email addresses.
            use_tls: Whether to use TLS encryption.
            logger: Optional logger instance.
        """
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.sender_email = sender_email or smtp_user
        self.recipients = recipients or []
        self.use_tls = use_tls
        self.logger = logger or logging.getLogger(__name__)
    
    @property
    def channel_name(self) -> str:
        return "email"
    
    def is_configured(self) -> bool:
        """Check if email channel is properly configured."""
        return bool(
            self.smtp_host and
            self.sender_email and
            self.recipients
        )
    
    def send(self, alert: Alert) -> AlertResult:
        """Send alert via email."""
        if not self.is_configured():
            return AlertResult(
                success=False,
                channel=self.channel_name,
                alert=alert,
                error_message="Email channel not properly configured"
            )
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"[{alert.severity.value.upper()}] {alert.title}"
            msg['From'] = self.sender_email
            msg['To'] = ', '.join(self.recipients)
            
            # Create plain text body
            text_body = self._format_text_body(alert)
            msg.attach(MIMEText(text_body, 'plain'))
            
            # Create HTML body
            html_body = self._format_html_body(alert)
            msg.attach(MIMEText(html_body, 'html'))
            
            # Send email
            if self.use_tls:
                context = ssl.create_default_context()
                with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                    server.starttls(context=context)
                    if self.smtp_user and self.smtp_password:
                        server.login(self.smtp_user, self.smtp_password)
                    server.sendmail(self.sender_email, self.recipients, msg.as_string())
            else:
                with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                    if self.smtp_user and self.smtp_password:
                        server.login(self.smtp_user, self.smtp_password)
                    server.sendmail(self.sender_email, self.recipients, msg.as_string())
            
            self.logger.info(f"Email alert sent successfully to {len(self.recipients)} recipients")
            return AlertResult(
                success=True,
                channel=self.channel_name,
                alert=alert
            )
            
        except smtplib.SMTPException as e:
            error_msg = f"SMTP error: {str(e)}"
            self.logger.error(error_msg)
            return AlertResult(
                success=False,
                channel=self.channel_name,
                alert=alert,
                error_message=error_msg
            )
        except Exception as e:
            error_msg = f"Email send error: {str(e)}"
            self.logger.error(error_msg)
            return AlertResult(
                success=False,
                channel=self.channel_name,
                alert=alert,
                error_message=error_msg
            )
    
    def _format_text_body(self, alert: Alert) -> str:
        """Format alert as plain text."""
        lines = [
            f"Alert: {alert.title}",
            f"Severity: {alert.severity.value.upper()}",
            f"Type: {alert.alert_type.value}",
            f"Time: {alert.timestamp.isoformat()}",
            "",
            "Message:",
            alert.message,
            ""
        ]
        
        if alert.context:
            lines.append("Context:")
            for key, value in alert.context.items():
                lines.append(f"  {key}: {value}")
        
        return '\n'.join(lines)
    
    def _format_html_body(self, alert: Alert) -> str:
        """Format alert as HTML."""
        severity_colors = {
            AlertSeverity.INFO: '#17a2b8',
            AlertSeverity.WARNING: '#ffc107',
            AlertSeverity.ERROR: '#dc3545',
            AlertSeverity.CRITICAL: '#721c24'
        }
        color = severity_colors.get(alert.severity, '#6c757d')
        
        context_html = ""
        if alert.context:
            context_items = ''.join(
                f"<li><strong>{k}:</strong> {v}</li>"
                for k, v in alert.context.items()
            )
            context_html = f"<h4>Context:</h4><ul>{context_items}</ul>"
        
        return f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <div style="border-left: 4px solid {color}; padding-left: 15px; margin: 20px 0;">
                <h2 style="color: {color};">{alert.title}</h2>
                <p><strong>Severity:</strong> {alert.severity.value.upper()}</p>
                <p><strong>Type:</strong> {alert.alert_type.value}</p>
                <p><strong>Time:</strong> {alert.timestamp.isoformat()}</p>
                <h4>Message:</h4>
                <p>{alert.message}</p>
                {context_html}
            </div>
            <hr>
            <p style="color: #6c757d; font-size: 12px;">
                This alert was generated by Trestle ETL Pipeline
            </p>
        </body>
        </html>
        """


class WebhookAlertChannel(AlertChannel):
    """Webhook alert delivery channel."""
    
    def __init__(
        self,
        webhook_url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize webhook alert channel.
        
        Args:
            webhook_url: URL to send webhook notifications to.
            headers: Optional custom headers for webhook requests.
            timeout: Request timeout in seconds.
            logger: Optional logger instance.
        """
        self.webhook_url = webhook_url
        self.headers = headers or {'Content-Type': 'application/json'}
        self.timeout = timeout
        self.logger = logger or logging.getLogger(__name__)
    
    @property
    def channel_name(self) -> str:
        return "webhook"
    
    def is_configured(self) -> bool:
        """Check if webhook channel is properly configured."""
        return bool(self.webhook_url)
    
    def send(self, alert: Alert) -> AlertResult:
        """Send alert via webhook."""
        if not self.is_configured():
            return AlertResult(
                success=False,
                channel=self.channel_name,
                alert=alert,
                error_message="Webhook URL not configured"
            )
        
        try:
            # Prepare payload
            payload = alert.to_json().encode('utf-8')
            
            # Create request
            request = Request(
                self.webhook_url,
                data=payload,
                headers=self.headers,
                method='POST'
            )
            
            # Send request
            with urlopen(request, timeout=self.timeout) as response:
                status_code = response.getcode()
                
                if 200 <= status_code < 300:
                    self.logger.info(f"Webhook alert sent successfully (status: {status_code})")
                    return AlertResult(
                        success=True,
                        channel=self.channel_name,
                        alert=alert
                    )
                else:
                    error_msg = f"Webhook returned status {status_code}"
                    self.logger.warning(error_msg)
                    return AlertResult(
                        success=False,
                        channel=self.channel_name,
                        alert=alert,
                        error_message=error_msg
                    )
        
        except HTTPError as e:
            error_msg = f"HTTP error: {e.code} - {e.reason}"
            self.logger.error(error_msg)
            return AlertResult(
                success=False,
                channel=self.channel_name,
                alert=alert,
                error_message=error_msg
            )
        except URLError as e:
            error_msg = f"URL error: {str(e.reason)}"
            self.logger.error(error_msg)
            return AlertResult(
                success=False,
                channel=self.channel_name,
                alert=alert,
                error_message=error_msg
            )
        except Exception as e:
            error_msg = f"Webhook send error: {str(e)}"
            self.logger.error(error_msg)
            return AlertResult(
                success=False,
                channel=self.channel_name,
                alert=alert,
                error_message=error_msg
            )


@dataclass
class AlertThreshold:
    """Configuration for alert thresholds."""
    quota_warning_percent: float = 0.75  # 75% usage
    quota_critical_percent: float = 0.90  # 90% usage
    error_count_warning: int = 5
    error_count_critical: int = 10
    response_time_warning_ms: float = 5000.0
    response_time_critical_ms: float = 10000.0


class AlertManager:
    """
    Manages alert delivery across multiple channels.
    
    Provides:
    - Multiple alert channel support (email, webhook)
    - Alert triggers for critical errors and quota limits
    - Configurable alert thresholds
    - Alert history tracking
    """
    
    def __init__(
        self,
        thresholds: Optional[AlertThreshold] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize AlertManager.
        
        Args:
            thresholds: Alert threshold configuration.
            logger: Optional logger instance.
        """
        self.thresholds = thresholds or AlertThreshold()
        self.logger = logger or logging.getLogger(__name__)
        
        self._channels: List[AlertChannel] = []
        self._alert_history: List[AlertResult] = []
        self._suppressed_alerts: Dict[str, datetime] = {}
        self._suppression_window_seconds = 300  # 5 minutes
    
    def add_channel(self, channel: AlertChannel) -> None:
        """Add an alert delivery channel."""
        if channel.is_configured():
            self._channels.append(channel)
            self.logger.info(f"Added alert channel: {channel.channel_name}")
        else:
            self.logger.warning(f"Channel {channel.channel_name} not properly configured, skipping")
    
    def remove_channel(self, channel_name: str) -> bool:
        """Remove an alert channel by name."""
        for i, channel in enumerate(self._channels):
            if channel.channel_name == channel_name:
                self._channels.pop(i)
                self.logger.info(f"Removed alert channel: {channel_name}")
                return True
        return False
    
    def get_channels(self) -> List[str]:
        """Get list of configured channel names."""
        return [c.channel_name for c in self._channels]
    
    def send_alert(
        self,
        alert: Alert,
        channels: Optional[List[str]] = None
    ) -> List[AlertResult]:
        """
        Send an alert through configured channels.
        
        Args:
            alert: Alert to send.
            channels: Optional list of specific channels to use.
            
        Returns:
            List of AlertResult for each channel.
        """
        results = []
        
        # Check for alert suppression
        suppression_key = f"{alert.alert_type.value}:{alert.title}"
        if self._is_suppressed(suppression_key):
            self.logger.debug(f"Alert suppressed: {suppression_key}")
            return results
        
        # Determine which channels to use
        target_channels = self._channels
        if channels:
            target_channels = [c for c in self._channels if c.channel_name in channels]
        
        if not target_channels:
            self.logger.warning("No alert channels configured")
            return results
        
        # Send to each channel
        for channel in target_channels:
            try:
                result = channel.send(alert)
                results.append(result)
                self._alert_history.append(result)
                
                if result.success:
                    self.logger.info(f"Alert sent via {channel.channel_name}: {alert.title}")
                else:
                    self.logger.error(
                        f"Failed to send alert via {channel.channel_name}: {result.error_message}"
                    )
            except Exception as e:
                error_result = AlertResult(
                    success=False,
                    channel=channel.channel_name,
                    alert=alert,
                    error_message=str(e)
                )
                results.append(error_result)
                self._alert_history.append(error_result)
                self.logger.error(f"Exception sending alert via {channel.channel_name}: {e}")
        
        # Update suppression
        self._suppressed_alerts[suppression_key] = datetime.now()
        
        return results
    
    def _is_suppressed(self, key: str) -> bool:
        """Check if an alert is currently suppressed."""
        if key not in self._suppressed_alerts:
            return False
        
        last_sent = self._suppressed_alerts[key]
        elapsed = (datetime.now() - last_sent).total_seconds()
        return elapsed < self._suppression_window_seconds
    
    # Convenience methods for common alert types
    def alert_quota_warning(
        self,
        quota_type: str,
        usage_percent: float,
        remaining: int,
        limit: int
    ) -> List[AlertResult]:
        """Send a quota warning alert."""
        alert = Alert(
            alert_type=AlertType.QUOTA_WARNING,
            severity=AlertSeverity.WARNING,
            title=f"{quota_type} Quota Warning",
            message=f"{quota_type} quota is at {usage_percent:.1f}% usage. "
                    f"{remaining}/{limit} remaining.",
            context={
                'quota_type': quota_type,
                'usage_percent': usage_percent,
                'remaining': remaining,
                'limit': limit
            }
        )
        return self.send_alert(alert)
    
    def alert_quota_critical(
        self,
        quota_type: str,
        usage_percent: float,
        remaining: int,
        limit: int
    ) -> List[AlertResult]:
        """Send a quota critical alert."""
        alert = Alert(
            alert_type=AlertType.QUOTA_CRITICAL,
            severity=AlertSeverity.CRITICAL,
            title=f"{quota_type} Quota Critical",
            message=f"{quota_type} quota is critically low at {usage_percent:.1f}% usage. "
                    f"Only {remaining}/{limit} remaining. Operations may be paused.",
            context={
                'quota_type': quota_type,
                'usage_percent': usage_percent,
                'remaining': remaining,
                'limit': limit
            }
        )
        return self.send_alert(alert)
    
    def alert_etl_error(
        self,
        error_message: str,
        error_type: str,
        context: Optional[Dict[str, Any]] = None
    ) -> List[AlertResult]:
        """Send an ETL error alert."""
        alert = Alert(
            alert_type=AlertType.ETL_ERROR,
            severity=AlertSeverity.ERROR,
            title="ETL Pipeline Error",
            message=error_message,
            context={
                'error_type': error_type,
                **(context or {})
            }
        )
        return self.send_alert(alert)
    
    def alert_etl_critical(
        self,
        error_message: str,
        error_type: str,
        context: Optional[Dict[str, Any]] = None
    ) -> List[AlertResult]:
        """Send a critical ETL error alert."""
        alert = Alert(
            alert_type=AlertType.ETL_CRITICAL,
            severity=AlertSeverity.CRITICAL,
            title="Critical ETL Pipeline Failure",
            message=error_message,
            context={
                'error_type': error_type,
                **(context or {})
            }
        )
        return self.send_alert(alert)
    
    def alert_database_error(
        self,
        error_message: str,
        operation: str,
        context: Optional[Dict[str, Any]] = None
    ) -> List[AlertResult]:
        """Send a database error alert."""
        alert = Alert(
            alert_type=AlertType.DATABASE_ERROR,
            severity=AlertSeverity.ERROR,
            title="Database Error",
            message=error_message,
            context={
                'operation': operation,
                **(context or {})
            }
        )
        return self.send_alert(alert)
    
    def alert_api_error(
        self,
        error_message: str,
        endpoint: str,
        status_code: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> List[AlertResult]:
        """Send an API error alert."""
        alert = Alert(
            alert_type=AlertType.API_ERROR,
            severity=AlertSeverity.ERROR,
            title="API Error",
            message=error_message,
            context={
                'endpoint': endpoint,
                'status_code': status_code,
                **(context or {})
            }
        )
        return self.send_alert(alert)
    
    # Threshold checking methods
    def check_quota_threshold(
        self,
        quota_type: str,
        remaining: int,
        limit: int
    ) -> Optional[List[AlertResult]]:
        """
        Check quota against thresholds and send alerts if needed.
        
        Returns:
            List of AlertResult if alert was sent, None otherwise.
        """
        if limit <= 0:
            return None
        
        usage_percent = ((limit - remaining) / limit) * 100
        
        if usage_percent >= self.thresholds.quota_critical_percent * 100:
            return self.alert_quota_critical(quota_type, usage_percent, remaining, limit)
        elif usage_percent >= self.thresholds.quota_warning_percent * 100:
            return self.alert_quota_warning(quota_type, usage_percent, remaining, limit)
        
        return None
    
    def get_alert_history(
        self,
        limit: Optional[int] = None,
        success_only: bool = False,
        failed_only: bool = False
    ) -> List[AlertResult]:
        """
        Get alert history.
        
        Args:
            limit: Maximum number of results to return.
            success_only: Only return successful alerts.
            failed_only: Only return failed alerts.
            
        Returns:
            List of AlertResult.
        """
        results = self._alert_history.copy()
        
        if success_only:
            results = [r for r in results if r.success]
        elif failed_only:
            results = [r for r in results if not r.success]
        
        if limit:
            results = results[-limit:]
        
        return results
    
    def clear_history(self) -> None:
        """Clear alert history."""
        self._alert_history.clear()
    
    def clear_suppression(self) -> None:
        """Clear alert suppression cache."""
        self._suppressed_alerts.clear()
    
    def set_suppression_window(self, seconds: int) -> None:
        """Set the alert suppression window in seconds."""
        self._suppression_window_seconds = seconds


def create_alert_manager_from_config(
    email_enabled: bool = False,
    email_recipients: Optional[List[str]] = None,
    smtp_host: Optional[str] = None,
    smtp_port: int = 587,
    smtp_user: Optional[str] = None,
    smtp_password: Optional[str] = None,
    webhook_enabled: bool = False,
    webhook_url: Optional[str] = None,
    thresholds: Optional[AlertThreshold] = None
) -> AlertManager:
    """
    Create an AlertManager from configuration parameters.
    
    Args:
        email_enabled: Whether to enable email alerts.
        email_recipients: List of email recipients.
        smtp_host: SMTP server hostname.
        smtp_port: SMTP server port.
        smtp_user: SMTP username.
        smtp_password: SMTP password.
        webhook_enabled: Whether to enable webhook alerts.
        webhook_url: Webhook URL.
        thresholds: Alert thresholds.
        
    Returns:
        Configured AlertManager instance.
    """
    manager = AlertManager(thresholds=thresholds)
    
    if email_enabled and smtp_host and email_recipients:
        email_channel = EmailAlertChannel(
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            smtp_user=smtp_user,
            smtp_password=smtp_password,
            recipients=email_recipients
        )
        manager.add_channel(email_channel)
    
    if webhook_enabled and webhook_url:
        webhook_channel = WebhookAlertChannel(webhook_url=webhook_url)
        manager.add_channel(webhook_channel)
    
    return manager
