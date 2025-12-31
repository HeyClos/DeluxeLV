"""
Property-based tests for OData Client.

Feature: trestle-etl-pipeline, Property 1: Authentication Success and Failure Handling
Validates: Requirements 1.1

Feature: trestle-etl-pipeline, Property 2: OData Query Construction  
Validates: Requirements 1.2

Feature: trestle-etl-pipeline, Property 4: Response Validation and Error Handling
Validates: Requirements 1.4
"""

import json
from unittest.mock import Mock, patch, MagicMock
from urllib.parse import parse_qs, urlparse

import pytest
import requests
from hypothesis import given, strategies as st, settings, assume

from trestle_etl.config import APIConfig
from trestle_etl.odata_client import ODataClient, AuthenticationError, ODataError


# Strategy for generating valid API credentials
api_credentials_strategy = st.tuples(
    st.text(min_size=1, max_size=100),  # client_id
    st.text(min_size=1, max_size=100)   # client_secret
)

# Strategy for generating OData entity set names
entity_set_strategy = st.sampled_from([
    'Property', 'Media', 'Member', 'Office', 'Listing'
])

# Strategy for generating field names
field_name_strategy = st.text(
    alphabet=st.sampled_from('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_'),
    min_size=1,
    max_size=50
)

# Strategy for generating OData filter expressions
filter_expr_strategy = st.one_of(
    st.none(),
    st.text(min_size=1, max_size=200)
)

# Strategy for generating select field lists
select_fields_strategy = st.one_of(
    st.none(),
    st.lists(field_name_strategy, min_size=1, max_size=10)
)

# Strategy for generating pagination parameters
pagination_strategy = st.tuples(
    st.one_of(st.none(), st.integers(min_value=1, max_value=1000)),  # top
    st.one_of(st.none(), st.integers(min_value=0, max_value=10000))  # skip
)


def create_mock_config(client_id: str = "test_id", client_secret: str = "test_secret") -> APIConfig:
    """Create a mock API configuration."""
    return APIConfig(
        client_id=client_id,
        client_secret=client_secret,
        base_url="https://api-test.example.com/odata",
        token_url="https://auth-test.example.com/token",
        timeout=30
    )


class TestAuthenticationSuccessAndFailureHandling:
    """
    Property 1: Authentication Success and Failure Handling
    
    For any set of API credentials, the authentication process should succeed 
    with valid credentials and fail gracefully with invalid credentials, 
    returning appropriate error messages.
    
    Validates: Requirements 1.1
    """

    @given(credentials=api_credentials_strategy)
    @settings(max_examples=20)
    def test_valid_credentials_return_token(self, credentials):
        """
        Feature: trestle-etl-pipeline, Property 1: Authentication Success and Failure Handling
        
        For any valid credentials, authentication should succeed and return a token.
        """
        client_id, client_secret = credentials
        config = create_mock_config(client_id, client_secret)
        client = ODataClient(config)
        
        # Mock successful authentication response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token_12345',
            'expires_in': 28800,
            'token_type': 'Bearer'
        }
        
        with patch.object(client._session, 'post', return_value=mock_response) as mock_post:
            token = client.authenticate()
            
            # Verify token is returned
            assert token == 'test_token_12345'
            
            # Verify correct authentication request was made
            mock_post.assert_called_once()
            call_args = mock_post.call_args
            
            # Check URL
            assert call_args[0][0] == config.token_url
            
            # Check request data
            request_data = call_args[1]['data']
            assert request_data['client_id'] == client_id
            assert request_data['client_secret'] == client_secret
            assert request_data['grant_type'] == 'client_credentials'
            assert request_data['scope'] == 'api'

    @given(credentials=api_credentials_strategy)
    @settings(max_examples=20)
    def test_invalid_credentials_raise_authentication_error(self, credentials):
        """
        Feature: trestle-etl-pipeline, Property 1: Authentication Success and Failure Handling
        
        For any invalid credentials, authentication should fail gracefully with 
        appropriate error messages.
        """
        client_id, client_secret = credentials
        config = create_mock_config(client_id, client_secret)
        client = ODataClient(config)
        
        # Mock authentication failure response
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.json.return_value = {
            'error': 'invalid_client',
            'error_description': 'Invalid client credentials'
        }
        
        with patch.object(client._session, 'post', return_value=mock_response):
            with pytest.raises(AuthenticationError) as exc_info:
                client.authenticate()
            
            # Verify error message is appropriate
            assert "Invalid client credentials" in str(exc_info.value)

    @given(credentials=api_credentials_strategy)
    @settings(max_examples=20)
    def test_network_errors_raise_authentication_error(self, credentials):
        """
        Feature: trestle-etl-pipeline, Property 1: Authentication Success and Failure Handling
        
        For any network error during authentication, should fail gracefully with 
        appropriate error messages.
        """
        client_id, client_secret = credentials
        config = create_mock_config(client_id, client_secret)
        client = ODataClient(config)
        
        # Mock network error
        with patch.object(client._session, 'post', side_effect=requests.ConnectionError("Network error")):
            with pytest.raises(AuthenticationError) as exc_info:
                client.authenticate()
            
            # Verify error message indicates network issue
            assert "Network error" in str(exc_info.value)

    @given(credentials=api_credentials_strategy)
    @settings(max_examples=20)
    def test_malformed_response_raises_authentication_error(self, credentials):
        """
        Feature: trestle-etl-pipeline, Property 1: Authentication Success and Failure Handling
        
        For any malformed authentication response, should fail gracefully.
        """
        client_id, client_secret = credentials
        config = create_mock_config(client_id, client_secret)
        client = ODataClient(config)
        
        # Mock response with invalid JSON
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        
        with patch.object(client._session, 'post', return_value=mock_response):
            with pytest.raises(AuthenticationError) as exc_info:
                client.authenticate()
            
            # Verify error message indicates JSON issue
            assert "Invalid JSON" in str(exc_info.value)

    @given(credentials=api_credentials_strategy)
    @settings(max_examples=20)
    def test_missing_access_token_raises_authentication_error(self, credentials):
        """
        Feature: trestle-etl-pipeline, Property 1: Authentication Success and Failure Handling
        
        For any response missing access token, should fail gracefully.
        """
        client_id, client_secret = credentials
        config = create_mock_config(client_id, client_secret)
        client = ODataClient(config)
        
        # Mock response without access token
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'expires_in': 28800,
            'token_type': 'Bearer'
            # Missing 'access_token'
        }
        
        with patch.object(client._session, 'post', return_value=mock_response):
            with pytest.raises(AuthenticationError) as exc_info:
                client.authenticate()
            
            # Verify error message indicates missing token
            assert "No access token" in str(exc_info.value)

    @given(credentials=api_credentials_strategy)
    @settings(max_examples=20)
    def test_token_caching_prevents_redundant_requests(self, credentials):
        """
        Feature: trestle-etl-pipeline, Property 1: Authentication Success and Failure Handling
        
        For any valid credentials, token caching should prevent redundant 
        authentication requests.
        """
        client_id, client_secret = credentials
        config = create_mock_config(client_id, client_secret)
        client = ODataClient(config)
        
        # Mock successful authentication response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'cached_token_12345',
            'expires_in': 28800,
            'token_type': 'Bearer'
        }
        
        with patch.object(client._session, 'post', return_value=mock_response) as mock_post:
            # First authentication call
            token1 = client.authenticate()
            
            # Second authentication call should use cached token
            token2 = client.authenticate()
            
            # Verify same token returned
            assert token1 == token2 == 'cached_token_12345'
            
            # Verify only one network request was made
            assert mock_post.call_count == 1


class TestODataQueryConstruction:
    """
    Property 2: OData Query Construction
    
    For any combination of field selections and filters, the OData client should 
    construct syntactically correct OData URLs that include all specified 
    parameters in the proper format.
    
    Validates: Requirements 1.2
    """

    @given(
        entity_set=entity_set_strategy,
        filter_expr=filter_expr_strategy,
        select_fields=select_fields_strategy,
        pagination=pagination_strategy
    )
    @settings(max_examples=50)
    def test_build_odata_url_includes_all_parameters(
        self, entity_set, filter_expr, select_fields, pagination
    ):
        """
        Feature: trestle-etl-pipeline, Property 2: OData Query Construction
        
        For any combination of OData parameters, the URL should include all 
        specified parameters in the correct format.
        """
        top, skip = pagination
        config = create_mock_config()
        client = ODataClient(config)
        
        url = client.build_odata_url(
            entity_set=entity_set,
            filter_expr=filter_expr,
            select_fields=select_fields,
            top=top,
            skip=skip
        )
        
        # Parse the URL
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        
        # Verify base URL structure
        assert entity_set in parsed.path
        assert parsed.scheme in ['http', 'https']
        
        # Verify filter parameter
        if filter_expr:
            assert '$filter' in query_params
            assert query_params['$filter'][0] == filter_expr
        else:
            assert '$filter' not in query_params
        
        # Verify select parameter
        if select_fields:
            assert '$select' in query_params
            expected_select = ','.join(select_fields)
            assert query_params['$select'][0] == expected_select
        else:
            assert '$select' not in query_params
        
        # Verify pagination parameters
        if top is not None:
            assert '$top' in query_params
            assert query_params['$top'][0] == str(top)
        else:
            assert '$top' not in query_params
        
        if skip is not None:
            assert '$skip' in query_params
            assert query_params['$skip'][0] == str(skip)
        else:
            assert '$skip' not in query_params

    @given(entity_set=entity_set_strategy)
    @settings(max_examples=20)
    def test_build_odata_url_without_parameters_is_valid(self, entity_set):
        """
        Feature: trestle-etl-pipeline, Property 2: OData Query Construction
        
        For any entity set without additional parameters, should construct 
        a valid base URL.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        url = client.build_odata_url(entity_set=entity_set)
        
        # Parse the URL
        parsed = urlparse(url)
        
        # Verify URL structure
        assert entity_set in parsed.path
        assert parsed.scheme in ['http', 'https']
        assert parsed.query == ''  # No query parameters

    @given(
        entity_set=entity_set_strategy,
        orderby=st.one_of(st.none(), st.text(min_size=1, max_size=100))
    )
    @settings(max_examples=20)
    def test_build_odata_url_with_orderby_parameter(self, entity_set, orderby):
        """
        Feature: trestle-etl-pipeline, Property 2: OData Query Construction
        
        For any orderby parameter, should be included correctly in the URL.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        url = client.build_odata_url(entity_set=entity_set, orderby=orderby)
        
        # Parse the URL
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        
        # Verify orderby parameter
        if orderby:
            assert '$orderby' in query_params
            assert query_params['$orderby'][0] == orderby
        else:
            assert '$orderby' not in query_params


class TestResponseValidationAndErrorHandling:
    """
    Property 4: Response Validation and Error Handling
    
    For any API response format (valid or invalid), the system should correctly 
    validate the response structure and handle errors without crashing.
    
    Validates: Requirements 1.4
    """

    @given(entity_set=entity_set_strategy)
    @settings(max_examples=20)
    def test_valid_json_response_is_parsed_correctly(self, entity_set):
        """
        Feature: trestle-etl-pipeline, Property 4: Response Validation and Error Handling
        
        For any valid JSON response, should parse and return data correctly.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        # Mock valid response
        mock_response_data = {
            'value': [{'id': 1, 'name': 'test'}],
            '@odata.count': 1
        }
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_response_data
        mock_response.headers = {'content-type': 'application/json'}
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', return_value=mock_response):
                result = client.execute_query(entity_set)
                
                # Verify response data is preserved
                assert result['value'] == mock_response_data['value']
                assert result['@odata.count'] == mock_response_data['@odata.count']
                
                # Verify response metadata is added
                assert '_response_headers' in result
                assert '_status_code' in result
                assert result['_status_code'] == 200

    @given(entity_set=entity_set_strategy)
    @settings(max_examples=20)
    def test_invalid_json_response_raises_odata_error(self, entity_set):
        """
        Feature: trestle-etl-pipeline, Property 4: Response Validation and Error Handling
        
        For any invalid JSON response, should raise appropriate error.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        # Mock response with invalid JSON
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', return_value=mock_response):
                with pytest.raises(ODataError) as exc_info:
                    client.execute_query(entity_set)
                
                # Verify error message indicates JSON issue
                assert "Invalid JSON" in str(exc_info.value)

    @given(
        entity_set=entity_set_strategy,
        status_code=st.sampled_from([400, 404, 500, 503])
    )
    @settings(max_examples=20)
    def test_http_error_responses_raise_odata_error(self, entity_set, status_code):
        """
        Feature: trestle-etl-pipeline, Property 4: Response Validation and Error Handling
        
        For any HTTP error response, should raise appropriate ODataError.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        # Mock error response
        mock_response = Mock()
        mock_response.status_code = status_code
        mock_response.text = f"Error {status_code}"
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', return_value=mock_response):
                with pytest.raises(ODataError) as exc_info:
                    client.execute_query(entity_set)
                
                # Verify error message includes status code
                assert str(status_code) in str(exc_info.value)

    @given(entity_set=entity_set_strategy)
    @settings(max_examples=20)
    def test_network_errors_raise_odata_error(self, entity_set):
        """
        Feature: trestle-etl-pipeline, Property 4: Response Validation and Error Handling
        
        For any network error, should raise appropriate ODataError.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', side_effect=requests.ConnectionError("Network error")):
                with pytest.raises(ODataError) as exc_info:
                    client.execute_query(entity_set)
                
                # Verify error message indicates network issue
                assert "Network error" in str(exc_info.value)

    @given(entity_set=entity_set_strategy)
    @settings(max_examples=20)
    def test_rate_limit_response_raises_specific_error(self, entity_set):
        """
        Feature: trestle-etl-pipeline, Property 4: Response Validation and Error Handling
        
        For any rate limit response (429), should raise specific error for 
        retry handling.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        # Mock rate limit response
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.text = "Rate limit exceeded"
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', return_value=mock_response):
                with pytest.raises(ODataError) as exc_info:
                    client.execute_query(entity_set)
                
                # Verify error message indicates rate limit
                assert "Rate limit exceeded" in str(exc_info.value)

    @given(entity_set=entity_set_strategy)
    @settings(max_examples=20)
    def test_authentication_retry_on_401_response(self, entity_set):
        """
        Feature: trestle-etl-pipeline, Property 4: Response Validation and Error Handling
        
        For any 401 response, should clear token cache and retry once.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        # Mock 401 response followed by successful response
        mock_401_response = Mock()
        mock_401_response.status_code = 401
        
        mock_success_response = Mock()
        mock_success_response.status_code = 200
        mock_success_response.json.return_value = {'value': []}
        mock_success_response.headers = {}
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', side_effect=[mock_401_response, mock_success_response]):
                result = client.execute_query(entity_set)
                
                # Verify successful result after retry
                assert result['value'] == []
                assert result['_status_code'] == 200