"""
OData Client for Trestle API.

Handles OAuth2 authentication and OData query construction/execution
for the CoreLogic Trestle API.
"""

import json
import time
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
    construction/execution.
    """
    
    def __init__(self, config: APIConfig):
        """
        Initialize OData client.
        
        Args:
            config: API configuration containing credentials and endpoints.
        """
        self.config = config
        self._token_cache = TokenCache()
        self._session = requests.Session()
        self._session.timeout = config.timeout
    
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
                # Rate limit - let caller handle retry logic
                raise ODataError(f"Rate limit exceeded: {response.text}")
            else:
                raise ODataError(f"Request failed: {response.status_code} - {response.text}")
                
        except requests.RequestException as e:
            raise ODataError(f"Network error during request: {str(e)}")
    
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
    
    def close(self) -> None:
        """Close the HTTP session."""
        self._session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()