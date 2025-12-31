"""
OData Client for Trestle API.

Handles OAuth2 authentication and OData query construction/execution
for the CoreLogic Trestle API.
"""

import json
import time
import random
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Union
from urllib.parse import urlencode, urljoin

import requests

from .config import APIConfig


class AuthenticationError(Exception):
    """Raised when authentication fails."""
    pass


class ODataError(Exception):
    """Raised when OData operations fail."""
    pass


class RateLimitError(Exception):
    """Raised when rate limits are exceeded."""
    pass


class TokenCache:
    """Simple token cache with expiration handling."""
    
    def __init__(self):
        self._token: Optional[str] = None
        self._expires_at: Optional[datetime] = None
    
    def get_token(self) -> Optional[str]:
        """Get cached token if still valid."""
        if self._token and self._expires_at:
            # Add 5 minute buffer before expiration
            if datetime.now() < (self._expires_at - timedelta(minutes=5)):
                return self._token
        return None
    
    def set_token(self, token: str, expires_in: int) -> None:
        """Cache token with expiration time."""
        self._token = token
        self._expires_at = datetime.now() + timedelta(seconds=expires_in)
    
    def clear(self) -> None:
        """Clear cached token."""
        self._token = None
        self._expires_at = None


class ODataClient:
    """
    OData client for CoreLogic Trestle API.
    
    Handles OAuth2 authentication with token caching and OData query
    construction/execution with rate limiting and exponential backoff.
    """
    
    def __init__(self, config: APIConfig, max_retries: int = 3, base_delay: float = 1.0):
        """
        Initialize OData client.
        
        Args:
            config: API configuration containing credentials and endpoints.
            max_retries: Maximum number of retry attempts for rate limits.
            base_delay: Base delay in seconds for exponential backoff.
        """
        self.config = config
        self.max_retries = max_retries
        self.base_delay = base_delay
        self._token_cache = TokenCache()
        self._session = requests.Session()
        self._session.timeout = config.timeout
        self._quota_info = {}
        self._last_quota_check = None
    
    def authenticate(self) -> str:
        """
        Authenticate with Trestle API using OAuth2 client credentials flow.
        
        Returns:
            Bearer token for API requests.
            
        Raises:
            AuthenticationError: If authentication fails.
        """
        # Check cache first
        cached_token = self._token_cache.get_token()
        if cached_token:
            return cached_token
        
        # Prepare authentication request
        auth_data = {
            'client_id': self.config.client_id,
            'client_secret': self.config.client_secret,
            'grant_type': 'client_credentials',
            'scope': 'api'
        }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }
        
        try:
            response = self._session.post(
                self.config.token_url,
                data=auth_data,
                headers=headers,
                timeout=self.config.timeout
            )
            
            if response.status_code == 200:
                token_data = response.json()
                access_token = token_data.get('access_token')
                expires_in = token_data.get('expires_in', 28800)  # Default 8 hours
                
                if not access_token:
                    raise AuthenticationError("No access token in response")
                
                # Cache the token
                self._token_cache.set_token(access_token, expires_in)
                return access_token
            
            elif response.status_code == 401:
                raise AuthenticationError("Invalid client credentials")
            elif response.status_code == 400:
                error_data = response.json() if response.content else {}
                error_desc = error_data.get('error_description', 'Bad request')
                raise AuthenticationError(f"Authentication failed: {error_desc}")
            else:
                raise AuthenticationError(
                    f"Authentication failed with status {response.status_code}: {response.text}"
                )
                
        except requests.RequestException as e:
            raise AuthenticationError(f"Network error during authentication: {str(e)}")
        except json.JSONDecodeError as e:
            raise AuthenticationError(f"Invalid JSON response during authentication: {str(e)}")
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Get headers with authentication token."""
        token = self.authenticate()
        return {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Retrieve API metadata information.
        
        Returns:
            Metadata response from the API.
            
        Raises:
            ODataError: If metadata request fails.
        """
        url = urljoin(self.config.base_url, '$metadata')
        headers = self._get_auth_headers()
        headers['Accept'] = 'application/xml'  # Metadata is typically XML
        
        try:
            response = self._session.get(url, headers=headers)
            
            if response.status_code == 200:
                return {'content': response.text, 'content_type': response.headers.get('content-type')}
            elif response.status_code == 401:
                # Clear token cache and retry once
                self._token_cache.clear()
                headers = self._get_auth_headers()
                headers['Accept'] = 'application/xml'
                response = self._session.get(url, headers=headers)
                
                if response.status_code == 200:
                    return {'content': response.text, 'content_type': response.headers.get('content-type')}
                else:
                    raise ODataError(f"Authentication failed after retry: {response.status_code}")
            else:
                raise ODataError(f"Metadata request failed: {response.status_code} - {response.text}")
                
        except requests.RequestException as e:
            raise ODataError(f"Network error during metadata request: {str(e)}")
    
    def build_odata_url(
        self,
        entity_set: str,
        filter_expr: Optional[str] = None,
        select_fields: Optional[List[str]] = None,
        top: Optional[int] = None,
        skip: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> str:
        """
        Build OData query URL with parameters.
        
        Args:
            entity_set: The OData entity set name (e.g., 'Property', 'Media').
            filter_expr: OData $filter expression.
            select_fields: List of fields to select.
            top: Maximum number of records to return.
            skip: Number of records to skip.
            orderby: OData $orderby expression.
            
        Returns:
            Complete OData URL with query parameters.
        """
        base_url = urljoin(self.config.base_url, entity_set)
        
        params = {}
        
        if filter_expr:
            params['$filter'] = filter_expr
        
        if select_fields:
            params['$select'] = ','.join(select_fields)
        
        if top is not None:
            params['$top'] = str(top)
        
        if skip is not None:
            params['$skip'] = str(skip)
        
        if orderby:
            params['$orderby'] = orderby
        
        if params:
            return f"{base_url}?{urlencode(params)}"
        else:
            return base_url
    
    def execute_query(
        self,
        entity_set: str,
        filter_expr: Optional[str] = None,
        select_fields: Optional[List[str]] = None,
        top: Optional[int] = None,
        skip: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Execute OData query and return results.
        
        Args:
            entity_set: The OData entity set name.
            filter_expr: OData $filter expression.
            select_fields: List of fields to select.
            top: Maximum number of records to return.
            skip: Number of records to skip.
            orderby: OData $orderby expression.
            
        Returns:
            OData response as dictionary.
            
        Raises:
            ODataError: If query execution fails.
        """
        url = self.build_odata_url(
            entity_set=entity_set,
            filter_expr=filter_expr,
            select_fields=select_fields,
            top=top,
            skip=skip,
            orderby=orderby
        )
        
        return self._execute_request(url)
    
    def execute_url(self, url: str) -> Dict[str, Any]:
        """
        Execute request to a specific URL (e.g., for pagination).
        
        Args:
            url: Complete URL to request.
            
        Returns:
            OData response as dictionary.
            
        Raises:
            ODataError: If request fails.
        """
        return self._execute_request(url)
    
    def _calculate_backoff_delay(self, attempt: int, base_delay: Optional[float] = None) -> float:
        """
        Calculate exponential backoff delay with jitter.
        
        Args:
            attempt: Current attempt number (0-based).
            base_delay: Base delay in seconds (uses instance default if None).
            
        Returns:
            Delay in seconds.
        """
        if base_delay is None:
            base_delay = self.base_delay
        
        # Exponential backoff: base_delay * (2 ^ attempt)
        delay = base_delay * (2 ** attempt)
        
        # Add jitter (±25% of delay)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        
        return max(0, delay + jitter)
    
    def _should_retry_on_quota(self, quota_info: Dict[str, Any]) -> bool:
        """
        Determine if request should be retried based on quota information.
        
        Args:
            quota_info: Quota information from response headers.
            
        Returns:
            True if request should be retried, False otherwise.
        """
        # Check minute quota
        minute_remaining = quota_info.get('minute_quota_remaining')
        if minute_remaining is not None and minute_remaining <= 0:
            return True
        
        # Check hour quota
        hour_remaining = quota_info.get('hour_quota_remaining')
        if hour_remaining is not None and hour_remaining <= 0:
            return True
        
        return False
    
    def _execute_with_retry(self, url: str) -> Dict[str, Any]:
        """
        Execute HTTP request with retry logic for rate limits.
        
        Args:
            url: URL to request.
            
        Returns:
            Response as dictionary.
            
        Raises:
            ODataError: If request fails after all retries.
            RateLimitError: If rate limits are consistently exceeded.
        """
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                response = self._execute_request_once(url)
                
                # Update quota information from response
                if '_response_headers' in response:
                    self._quota_info = self.get_quota_info(response['_response_headers'])
                    self._last_quota_check = datetime.now()
                
                return response
                
            except ODataError as e:
                last_exception = e
                
                # Check if this is a rate limit error (429)
                if "Rate limit exceeded" in str(e) or "429" in str(e):
                    if attempt < self.max_retries:
                        # Calculate backoff delay
                        delay = self._calculate_backoff_delay(attempt)
                        
                        # Wait before retry
                        time.sleep(delay)
                        continue
                    else:
                        # Max retries exceeded for rate limit
                        raise RateLimitError(f"Rate limit exceeded after {self.max_retries} retries: {str(e)}")
                else:
                    # Non-rate-limit error, don't retry
                    raise
        
        # Should not reach here, but just in case
        if last_exception:
            raise last_exception
        else:
            raise ODataError("Request failed after retries")
    
    def _execute_request_once(self, url: str) -> Dict[str, Any]:
        """
        Execute HTTP request once without retry logic.
        
        Args:
            url: URL to request.
            
        Returns:
            Response as dictionary.
            
        Raises:
            ODataError: If request fails.
        """
        headers = self._get_auth_headers()
        
        try:
            response = self._session.get(url, headers=headers)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    # Add response metadata
                    data['_response_headers'] = dict(response.headers)
                    data['_status_code'] = response.status_code
                    return data
                except json.JSONDecodeError as e:
                    raise ODataError(f"Invalid JSON response: {str(e)}")
            
            elif response.status_code == 401:
                # Clear token cache and retry once
                self._token_cache.clear()
                headers = self._get_auth_headers()
                response = self._session.get(url, headers=headers)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        data['_response_headers'] = dict(response.headers)
                        data['_status_code'] = response.status_code
                        return data
                    except json.JSONDecodeError as e:
                        raise ODataError(f"Invalid JSON response after retry: {str(e)}")
                else:
                    raise ODataError(f"Authentication failed after retry: {response.status_code}")
            
            elif response.status_code == 400:
                raise ODataError(f"Bad request: {response.text}")
            elif response.status_code == 404:
                raise ODataError(f"Resource not found (404): {url}")
            elif response.status_code == 429:
                # Rate limit - include response text for retry logic
                raise ODataError(f"Rate limit exceeded: {response.text}")
            else:
                raise ODataError(f"Request failed: {response.status_code} - {response.text}")
                
        except requests.RequestException as e:
            raise ODataError(f"Network error during request: {str(e)}")
    
    def _execute_request(self, url: str) -> Dict[str, Any]:
        """
        Execute HTTP request with authentication and error handling.
        
        Args:
            url: URL to request.
            
        Returns:
            Response as dictionary.
            
        Raises:
            ODataError: If request fails.
        """
        return self._execute_with_retry(url)
    
    def execute_paginated_query(
        self,
        entity_set: str,
        filter_expr: Optional[str] = None,
        select_fields: Optional[List[str]] = None,
        top: Optional[int] = None,
        skip: Optional[int] = None,
        orderby: Optional[str] = None,
        max_pages: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute OData query with automatic pagination handling.
        
        Args:
            entity_set: The OData entity set name.
            filter_expr: OData $filter expression.
            select_fields: List of fields to select.
            top: Maximum number of records per page (up to 1000).
            skip: Number of records to skip.
            orderby: OData $orderby expression.
            max_pages: Maximum number of pages to retrieve (None for all).
            
        Returns:
            List of all records from all pages.
            
        Raises:
            ODataError: If query execution fails.
        """
        all_records = []
        page_count = 0
        
        # Ensure top doesn't exceed 1000 (API limit)
        if top is not None and top > 1000:
            top = 1000
        
        # Execute initial query
        response = self.execute_query(
            entity_set=entity_set,
            filter_expr=filter_expr,
            select_fields=select_fields,
            top=top,
            skip=skip,
            orderby=orderby
        )
        
        # Process first page
        if 'value' in response:
            all_records.extend(response['value'])
        
        page_count += 1
        
        # Handle pagination with @odata.nextLink
        while '@odata.nextLink' in response:
            # Check max_pages limit
            if max_pages is not None and page_count >= max_pages:
                break
            
            next_url = response['@odata.nextLink']
            response = self.execute_url(next_url)
            
            # Process next page
            if 'value' in response:
                all_records.extend(response['value'])
            
            page_count += 1
        
        return all_records
    
    def get_quota_info(self, response_headers: Dict[str, str]) -> Dict[str, Any]:
        """
        Extract quota information from response headers.
        
        Args:
            response_headers: HTTP response headers.
            
        Returns:
            Dictionary with quota information.
        """
        quota_info = {}
        
        # Common quota header names
        quota_headers = [
            'Minute-Quota-Limit',
            'Minute-Quota-Remaining',
            'Hour-Quota-Limit', 
            'Hour-Quota-Remaining',
            'Daily-Quota-Limit',
            'Daily-Quota-Remaining'
        ]
        
        for header in quota_headers:
            value = response_headers.get(header)
            if value:
                try:
                    quota_info[header.lower().replace('-', '_')] = int(value)
                except ValueError:
                    quota_info[header.lower().replace('-', '_')] = value
        
        return quota_info
    
    def get_current_quota_info(self) -> Dict[str, Any]:
        """
        Get the most recent quota information.
        
        Returns:
            Dictionary with quota information and last check time.
        """
        return {
            'quota_info': self._quota_info.copy(),
            'last_check': self._last_quota_check
        }
    
    def is_quota_approaching_limit(self, threshold_percent: float = 0.1) -> Dict[str, bool]:
        """
        Check if quota usage is approaching limits.
        
        Args:
            threshold_percent: Threshold as percentage (0.1 = 10% remaining).
            
        Returns:
            Dictionary indicating which quotas are approaching limits.
        """
        alerts = {}
        
        # Check minute quota
        minute_limit = self._quota_info.get('minute_quota_limit')
        minute_remaining = self._quota_info.get('minute_quota_remaining')
        if minute_limit and minute_remaining is not None:
            remaining_percent = minute_remaining / minute_limit
            alerts['minute_quota_low'] = remaining_percent <= threshold_percent
        
        # Check hour quota
        hour_limit = self._quota_info.get('hour_quota_limit')
        hour_remaining = self._quota_info.get('hour_quota_remaining')
        if hour_limit and hour_remaining is not None:
            remaining_percent = hour_remaining / hour_limit
            alerts['hour_quota_low'] = remaining_percent <= threshold_percent
        
        # Check daily quota
        daily_limit = self._quota_info.get('daily_quota_limit')
        daily_remaining = self._quota_info.get('daily_quota_remaining')
        if daily_limit and daily_remaining is not None:
            remaining_percent = daily_remaining / daily_limit
            alerts['daily_quota_low'] = remaining_percent <= threshold_percent
        
        return alerts
    
    def close(self) -> None:
        """Close the HTTP session."""
        self._session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()