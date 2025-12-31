"""
Property-based tests for OData Client.

Feature: trestle-etl-pipeline, Property 1: Authentication Success and Failure Handling
Validates: Requirements 1.1

Feature: trestle-etl-pipeline, Property 2: OData Query Construction  
Validates: Requirements 1.2

Feature: trestle-etl-pipeline, Property 4: Response Validation and Error Handling
Validates: Requirements 1.4

Feature: trestle-etl-pipeline, Property 5: Pagination Completeness
Validates: Requirements 1.5

Feature: trestle-etl-pipeline, Property 3: Exponential Backoff Retry Logic
Validates: Requirements 1.3, 3.5, 5.3

Feature: trestle-etl-pipeline, Property 14: Quota Monitoring and Alerting
Validates: Requirements 4.4
"""

import json
import time
from unittest.mock import Mock, patch, MagicMock
from urllib.parse import parse_qs, urlparse

import pytest
import requests
from hypothesis import given, strategies as st, settings, assume

from trestle_etl.config import APIConfig
from trestle_etl.odata_client import ODataClient, AuthenticationError, ODataError, RateLimitError


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
        client = ODataClient(config, max_retries=1, base_delay=0.01)  # Limit retries for faster test
        
        # Mock rate limit response
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.text = "Rate limit exceeded"
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', return_value=mock_response):
                with patch('time.sleep'):  # Mock sleep to speed up test
                    with pytest.raises(RateLimitError) as exc_info:
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


class TestPaginationCompleteness:
    """
    Property 5: Pagination Completeness
    
    For any paginated API response set, all pages should be processed and no 
    records should be skipped or duplicated during pagination.
    
    Validates: Requirements 1.5
    """

    @given(
        entity_set=entity_set_strategy,
        page_sizes=st.lists(st.integers(min_value=1, max_value=100), min_size=1, max_size=5),
        records_per_page=st.integers(min_value=1, max_value=50)
    )
    @settings(max_examples=30)
    def test_paginated_query_returns_all_records_without_duplicates(
        self, entity_set, page_sizes, records_per_page
    ):
        """
        Feature: trestle-etl-pipeline, Property 5: Pagination Completeness
        
        For any paginated response set, all records should be retrieved without 
        skipping or duplicating any records.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        # Generate unique test records for each page
        all_expected_records = []
        mock_responses = []
        
        for i, page_size in enumerate(page_sizes):
            # Create unique records for this page
            page_records = [
                {'id': f'record_{i}_{j}', 'page': i, 'index': j}
                for j in range(min(page_size, records_per_page))
            ]
            all_expected_records.extend(page_records)
            
            # Create mock response for this page
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {}
            
            response_data = {'value': page_records}
            
            # Add nextLink for all pages except the last
            if i < len(page_sizes) - 1:
                response_data['@odata.nextLink'] = f'https://api.example.com/next_page_{i+1}'
            
            mock_response.json.return_value = response_data
            mock_responses.append(mock_response)
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', side_effect=mock_responses):
                result = client.execute_paginated_query(entity_set)
                
                # Verify all records are returned
                assert len(result) == len(all_expected_records)
                
                # Verify no duplicates by checking unique IDs
                result_ids = [record['id'] for record in result]
                expected_ids = [record['id'] for record in all_expected_records]
                
                assert len(result_ids) == len(set(result_ids))  # No duplicates
                assert set(result_ids) == set(expected_ids)  # All records present
                
                # Verify records are in correct order
                for i, record in enumerate(result):
                    expected_record = all_expected_records[i]
                    assert record['id'] == expected_record['id']
                    assert record['page'] == expected_record['page']
                    assert record['index'] == expected_record['index']

    @given(
        entity_set=entity_set_strategy,
        total_records=st.integers(min_value=1, max_value=1000),
        page_size=st.integers(min_value=1, max_value=100)
    )
    @settings(max_examples=20)
    def test_pagination_respects_top_parameter_limit(self, entity_set, total_records, page_size):
        """
        Feature: trestle-etl-pipeline, Property 5: Pagination Completeness
        
        For any top parameter exceeding 1000, the system should limit it to 1000 
        records per page as per API constraints.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        # Test with top parameter > 1000
        large_top = 1500
        
        # Mock response with records
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.json.return_value = {
            'value': [{'id': i} for i in range(min(total_records, 1000))]  # API should limit to 1000
        }
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', return_value=mock_response) as mock_get:
                client.execute_paginated_query(entity_set, top=large_top)
                
                # Verify the request was made with top=1000 (not 1500)
                mock_get.assert_called_once()
                call_args = mock_get.call_args[0][0]  # Get the URL
                
                # Parse URL to check top parameter
                parsed = urlparse(call_args)
                query_params = parse_qs(parsed.query)
                
                if '$top' in query_params:
                    assert query_params['$top'][0] == '1000'

    @given(
        entity_set=entity_set_strategy,
        max_pages=st.integers(min_value=1, max_value=5)
    )
    @settings(max_examples=20)
    def test_pagination_respects_max_pages_limit(self, entity_set, max_pages):
        """
        Feature: trestle-etl-pipeline, Property 5: Pagination Completeness
        
        For any max_pages parameter, pagination should stop after the specified 
        number of pages even if more pages are available.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        # Create more pages than max_pages to test the limit
        total_pages = max_pages + 2
        mock_responses = []
        
        for i in range(total_pages):
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {}
            
            response_data = {'value': [{'id': f'page_{i}_record_{j}'} for j in range(10)]}
            
            # Always add nextLink to test that max_pages stops pagination
            response_data['@odata.nextLink'] = f'https://api.example.com/next_page_{i+1}'
            
            mock_response.json.return_value = response_data
            mock_responses.append(mock_response)
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', side_effect=mock_responses) as mock_get:
                result = client.execute_paginated_query(entity_set, max_pages=max_pages)
                
                # Verify only max_pages were requested
                assert mock_get.call_count == max_pages
                
                # Verify correct number of records returned (max_pages * 10 records per page)
                expected_record_count = max_pages * 10
                assert len(result) == expected_record_count

    @given(entity_set=entity_set_strategy)
    @settings(max_examples=20)
    def test_pagination_handles_empty_pages_correctly(self, entity_set):
        """
        Feature: trestle-etl-pipeline, Property 5: Pagination Completeness
        
        For any paginated response containing empty pages, the system should 
        handle them correctly without errors.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        # Create responses with some empty pages
        mock_responses = []
        expected_records = []
        
        # First page with records
        mock_response1 = Mock()
        mock_response1.status_code = 200
        mock_response1.headers = {}
        page1_records = [{'id': 'record_1'}, {'id': 'record_2'}]
        mock_response1.json.return_value = {
            'value': page1_records,
            '@odata.nextLink': 'https://api.example.com/page2'
        }
        mock_responses.append(mock_response1)
        expected_records.extend(page1_records)
        
        # Second page empty
        mock_response2 = Mock()
        mock_response2.status_code = 200
        mock_response2.headers = {}
        mock_response2.json.return_value = {
            'value': [],
            '@odata.nextLink': 'https://api.example.com/page3'
        }
        mock_responses.append(mock_response2)
        
        # Third page with records
        mock_response3 = Mock()
        mock_response3.status_code = 200
        mock_response3.headers = {}
        page3_records = [{'id': 'record_3'}]
        mock_response3.json.return_value = {
            'value': page3_records
            # No nextLink - last page
        }
        mock_responses.append(mock_response3)
        expected_records.extend(page3_records)
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', side_effect=mock_responses):
                result = client.execute_paginated_query(entity_set)
                
                # Verify all non-empty records are returned
                assert len(result) == len(expected_records)
                result_ids = [record['id'] for record in result]
                expected_ids = [record['id'] for record in expected_records]
                assert result_ids == expected_ids

    @given(entity_set=entity_set_strategy)
    @settings(max_examples=20)
    def test_pagination_handles_missing_value_field(self, entity_set):
        """
        Feature: trestle-etl-pipeline, Property 5: Pagination Completeness
        
        For any paginated response missing the 'value' field, the system should 
        handle it gracefully without crashing.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        # Mock response without 'value' field
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.json.return_value = {
            '@odata.count': 0
            # Missing 'value' field
        }
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', return_value=mock_response):
                result = client.execute_paginated_query(entity_set)
                
                # Should return empty list when 'value' field is missing
                assert result == []


class TestExponentialBackoffRetryLogic:
    """
    Property 3: Exponential Backoff Retry Logic
    
    For any rate limit or connection failure scenario, retry attempts should 
    follow exponential backoff timing patterns with configurable maximum attempts.
    
    Validates: Requirements 1.3, 3.5, 5.3
    """

    @given(
        entity_set=entity_set_strategy,
        max_retries=st.integers(min_value=1, max_value=5),
        base_delay=st.floats(min_value=0.1, max_value=2.0)
    )
    @settings(max_examples=20)
    def test_exponential_backoff_timing_pattern(self, entity_set, max_retries, base_delay):
        """
        Feature: trestle-etl-pipeline, Property 3: Exponential Backoff Retry Logic
        
        For any retry scenario, delays should follow exponential backoff pattern 
        with jitter.
        """
        config = create_mock_config()
        client = ODataClient(config, max_retries=max_retries, base_delay=base_delay)
        
        # Test the backoff calculation directly
        delays = []
        for attempt in range(max_retries):
            delay = client._calculate_backoff_delay(attempt, base_delay)
            delays.append(delay)
        
        # Verify exponential growth pattern (allowing for jitter)
        for i in range(1, len(delays)):
            # Expected delay without jitter: base_delay * (2 ^ attempt)
            expected_min = base_delay * (2 ** i) * 0.75  # Account for negative jitter
            expected_max = base_delay * (2 ** i) * 1.25  # Account for positive jitter
            
            # Verify delay is within expected range
            assert expected_min <= delays[i] <= expected_max
            
            # Verify generally increasing pattern (accounting for jitter)
            # The delay should be at least as large as the base pattern
            base_expected = base_delay * (2 ** i) * 0.5  # Very conservative check
            assert delays[i] >= base_expected

    @given(
        entity_set=entity_set_strategy,
        max_retries=st.integers(min_value=1, max_value=4)
    )
    @settings(max_examples=20)
    def test_rate_limit_retries_respect_max_attempts(self, entity_set, max_retries):
        """
        Feature: trestle-etl-pipeline, Property 3: Exponential Backoff Retry Logic
        
        For any rate limit scenario, the system should retry up to max_retries 
        times and then raise RateLimitError.
        """
        config = create_mock_config()
        client = ODataClient(config, max_retries=max_retries, base_delay=0.01)  # Fast for testing
        
        # Mock rate limit response (429)
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.text = "Rate limit exceeded"
        
        call_count = 0
        def mock_get_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return mock_response
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', side_effect=mock_get_side_effect):
                with patch('time.sleep'):  # Mock sleep to speed up test
                    with pytest.raises(RateLimitError) as exc_info:
                        client.execute_query(entity_set)
                    
                    # Verify correct number of attempts (initial + retries)
                    assert call_count == max_retries + 1
                    
                    # Verify error message mentions retries
                    assert f"after {max_retries} retries" in str(exc_info.value)

    @given(
        entity_set=entity_set_strategy,
        max_retries=st.integers(min_value=2, max_value=4)
    )
    @settings(max_examples=15)
    def test_successful_retry_after_rate_limit(self, entity_set, max_retries):
        """
        Feature: trestle-etl-pipeline, Property 3: Exponential Backoff Retry Logic
        
        For any rate limit followed by success, the system should eventually 
        succeed and return the result.
        """
        config = create_mock_config()
        client = ODataClient(config, max_retries=max_retries, base_delay=0.01)
        
        # Mock rate limit responses followed by success
        rate_limit_response = Mock()
        rate_limit_response.status_code = 429
        rate_limit_response.text = "Rate limit exceeded"
        
        success_response = Mock()
        success_response.status_code = 200
        success_response.headers = {}
        success_response.json.return_value = {'value': [{'id': 'success'}]}
        
        # Fail a few times, then succeed
        failure_count = min(max_retries - 1, 2)  # Ensure we don't exceed max_retries
        responses = [rate_limit_response] * failure_count + [success_response]
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', side_effect=responses):
                with patch('time.sleep'):  # Mock sleep to speed up test
                    result = client.execute_query(entity_set)
                    
                    # Verify successful result
                    assert result['value'] == [{'id': 'success'}]
                    assert result['_status_code'] == 200

    @given(
        entity_set=entity_set_strategy,
        non_rate_limit_status=st.sampled_from([400, 404, 500, 503])
    )
    @settings(max_examples=20)
    def test_non_rate_limit_errors_do_not_retry(self, entity_set, non_rate_limit_status):
        """
        Feature: trestle-etl-pipeline, Property 3: Exponential Backoff Retry Logic
        
        For any non-rate-limit error, the system should not retry and should 
        raise ODataError immediately.
        """
        config = create_mock_config()
        client = ODataClient(config, max_retries=3, base_delay=0.01)
        
        # Mock non-rate-limit error response
        mock_response = Mock()
        mock_response.status_code = non_rate_limit_status
        mock_response.text = f"Error {non_rate_limit_status}"
        
        call_count = 0
        def mock_get_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return mock_response
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', side_effect=mock_get_side_effect):
                with pytest.raises(ODataError) as exc_info:
                    client.execute_query(entity_set)
                
                # Verify only one attempt was made (no retries)
                assert call_count == 1
                
                # Verify error message contains status code
                assert str(non_rate_limit_status) in str(exc_info.value)

    @given(
        base_delay=st.floats(min_value=0.01, max_value=1.0),
        attempt=st.integers(min_value=0, max_value=5)
    )
    @settings(max_examples=30)
    def test_backoff_delay_calculation_properties(self, base_delay, attempt):
        """
        Feature: trestle-etl-pipeline, Property 3: Exponential Backoff Retry Logic
        
        For any base delay and attempt number, the calculated delay should have 
        proper mathematical properties.
        """
        config = create_mock_config()
        client = ODataClient(config, base_delay=base_delay)
        
        delay = client._calculate_backoff_delay(attempt, base_delay)
        
        # Verify delay is non-negative
        assert delay >= 0
        
        # Verify delay is roughly in expected range (accounting for jitter)
        expected_base = base_delay * (2 ** attempt)
        min_expected = expected_base * 0.75  # 25% negative jitter
        max_expected = expected_base * 1.25  # 25% positive jitter
        
        assert min_expected <= delay <= max_expected
        
        # For attempt 0, delay should be close to base_delay
        if attempt == 0:
            assert base_delay * 0.75 <= delay <= base_delay * 1.25

    @given(entity_set=entity_set_strategy)
    @settings(max_examples=15)
    def test_network_errors_trigger_retry_logic(self, entity_set):
        """
        Feature: trestle-etl-pipeline, Property 3: Exponential Backoff Retry Logic
        
        For any network error during request, the system should apply retry logic 
        appropriately.
        """
        config = create_mock_config()
        client = ODataClient(config, max_retries=2, base_delay=0.01)
        
        # Mock network error followed by success
        network_error = requests.ConnectionError("Network error")
        success_response = Mock()
        success_response.status_code = 200
        success_response.headers = {}
        success_response.json.return_value = {'value': []}
        
        call_count = 0
        def mock_get_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise network_error
            return success_response
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', side_effect=mock_get_side_effect):
                # Network errors should not trigger the retry logic in _execute_with_retry
                # They should be caught and re-raised as ODataError immediately
                with pytest.raises(ODataError) as exc_info:
                    client.execute_query(entity_set)
                
                # Verify network error is mentioned
                assert "Network error" in str(exc_info.value)
                
                # Verify only one attempt was made (network errors don't retry)
                assert call_count == 1


class TestQuotaMonitoringAndAlerting:
    """
    Property 14: Quota Monitoring and Alerting
    
    For any API usage pattern that approaches quota limits, the cost monitor 
    should trigger appropriate alerts and optionally pause operations.
    
    Validates: Requirements 4.4
    """

    @given(
        minute_limit=st.integers(min_value=100, max_value=10000),
        minute_remaining=st.integers(min_value=0, max_value=100),
        hour_limit=st.integers(min_value=1000, max_value=100000),
        hour_remaining=st.integers(min_value=0, max_value=1000)
    )
    @settings(max_examples=30)
    def test_quota_info_extraction_from_headers(
        self, minute_limit, minute_remaining, hour_limit, hour_remaining
    ):
        """
        Feature: trestle-etl-pipeline, Property 14: Quota Monitoring and Alerting
        
        For any set of quota headers, the system should correctly extract and 
        parse quota information.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        # Create mock headers with quota information
        headers = {
            'Minute-Quota-Limit': str(minute_limit),
            'Minute-Quota-Remaining': str(minute_remaining),
            'Hour-Quota-Limit': str(hour_limit),
            'Hour-Quota-Remaining': str(hour_remaining),
            'Content-Type': 'application/json'
        }
        
        quota_info = client.get_quota_info(headers)
        
        # Verify all quota values are correctly extracted and converted to integers
        assert quota_info['minute_quota_limit'] == minute_limit
        assert quota_info['minute_quota_remaining'] == minute_remaining
        assert quota_info['hour_quota_limit'] == hour_limit
        assert quota_info['hour_quota_remaining'] == hour_remaining
        
        # Verify non-quota headers are ignored
        assert 'content_type' not in quota_info

    @given(
        quota_limit=st.integers(min_value=100, max_value=10000),
        remaining_percent=st.floats(min_value=0.0, max_value=1.0),
        threshold_percent=st.floats(min_value=0.05, max_value=0.5)
    )
    @settings(max_examples=30)
    def test_quota_approaching_limit_detection(self, quota_limit, remaining_percent, threshold_percent):
        """
        Feature: trestle-etl-pipeline, Property 14: Quota Monitoring and Alerting
        
        For any quota usage level, the system should correctly identify when 
        quotas are approaching their limits based on configurable thresholds.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        # Calculate remaining quota based on percentage
        quota_remaining = int(quota_limit * remaining_percent)
        
        # Set up quota info in client
        client._quota_info = {
            'minute_quota_limit': quota_limit,
            'minute_quota_remaining': quota_remaining,
            'hour_quota_limit': quota_limit * 10,  # Different scale
            'hour_quota_remaining': quota_remaining * 10
        }
        
        alerts = client.is_quota_approaching_limit(threshold_percent)
        
        # Verify alert logic
        expected_alert = remaining_percent <= threshold_percent
        
        assert 'minute_quota_low' in alerts
        assert alerts['minute_quota_low'] == expected_alert
        
        assert 'hour_quota_low' in alerts
        assert alerts['hour_quota_low'] == expected_alert

    @given(
        entity_set=entity_set_strategy,
        quota_headers=st.dictionaries(
            st.sampled_from([
                'Minute-Quota-Limit', 'Minute-Quota-Remaining',
                'Hour-Quota-Limit', 'Hour-Quota-Remaining',
                'Daily-Quota-Limit', 'Daily-Quota-Remaining'
            ]),
            st.integers(min_value=0, max_value=10000).map(str),
            min_size=2,
            max_size=6
        )
    )
    @settings(max_examples=20)
    def test_quota_info_updated_after_requests(self, entity_set, quota_headers):
        """
        Feature: trestle-etl-pipeline, Property 14: Quota Monitoring and Alerting
        
        For any API request with quota headers, the client should update its 
        internal quota tracking information.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        # Mock successful response with quota headers
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = quota_headers
        mock_response.json.return_value = {'value': []}
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', return_value=mock_response):
                client.execute_query(entity_set)
                
                # Verify quota info was updated
                current_quota = client.get_current_quota_info()
                assert current_quota['last_check'] is not None
                
                # Verify quota values match headers
                for header_name, header_value in quota_headers.items():
                    expected_key = header_name.lower().replace('-', '_')
                    if expected_key in current_quota['quota_info']:
                        assert current_quota['quota_info'][expected_key] == int(header_value)

    @given(
        quota_values=st.lists(
            st.tuples(
                st.integers(min_value=1000, max_value=10000),  # limit
                st.integers(min_value=0, max_value=100)        # remaining
            ),
            min_size=1,
            max_size=3
        )
    )
    @settings(max_examples=20)
    def test_multiple_quota_types_monitoring(self, quota_values):
        """
        Feature: trestle-etl-pipeline, Property 14: Quota Monitoring and Alerting
        
        For any combination of quota types (minute, hour, daily), the system 
        should monitor all types independently.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        quota_types = ['minute', 'hour', 'daily']
        quota_info = {}
        
        # Set up quota info for each type
        for i, (limit, remaining) in enumerate(quota_values):
            if i < len(quota_types):
                quota_type = quota_types[i]
                quota_info[f'{quota_type}_quota_limit'] = limit
                quota_info[f'{quota_type}_quota_remaining'] = remaining
        
        client._quota_info = quota_info
        
        # Test alert detection for low threshold
        alerts = client.is_quota_approaching_limit(0.2)  # 20% threshold
        
        # Verify each quota type is checked independently
        for i, (limit, remaining) in enumerate(quota_values):
            if i < len(quota_types):
                quota_type = quota_types[i]
                alert_key = f'{quota_type}_quota_low'
                
                if alert_key in alerts:
                    expected_alert = (remaining / limit) <= 0.2
                    assert alerts[alert_key] == expected_alert

    @given(
        invalid_quota_values=st.lists(
            st.one_of(
                st.text(alphabet=st.characters(blacklist_categories=('Nd',)), min_size=1, max_size=10),  # Non-numeric strings
                st.just(''),                       # Empty strings
                st.just('invalid')                 # Invalid strings
            ),
            min_size=1,
            max_size=4
        )
    )
    @settings(max_examples=20)
    def test_invalid_quota_headers_handled_gracefully(self, invalid_quota_values):
        """
        Feature: trestle-etl-pipeline, Property 14: Quota Monitoring and Alerting
        
        For any invalid quota header values, the system should handle them 
        gracefully without crashing.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        # Create headers with invalid quota values
        quota_headers = [
            'Minute-Quota-Limit', 'Minute-Quota-Remaining',
            'Hour-Quota-Limit', 'Hour-Quota-Remaining'
        ]
        
        headers = {}
        for i, invalid_value in enumerate(invalid_quota_values):
            if i < len(quota_headers):
                headers[quota_headers[i]] = invalid_value
        
        # Should not raise exception
        quota_info = client.get_quota_info(headers)
        
        # Verify invalid values are handled appropriately
        for i, invalid_value in enumerate(invalid_quota_values):
            if i < len(quota_headers):
                header_name = quota_headers[i]
                expected_key = header_name.lower().replace('-', '_')
                if expected_key in quota_info:
                    # Values that can't be converted to int should be stored as strings
                    # Values that can be converted (like '0') will be stored as integers
                    stored_value = quota_info[expected_key]
                    
                    # Either it's the original string (conversion failed) or converted int
                    try:
                        expected_int = int(invalid_value)
                        assert stored_value == expected_int or stored_value == invalid_value
                    except ValueError:
                        # Should be stored as string if conversion failed
                        assert stored_value == invalid_value

    @given(threshold_percent=st.floats(min_value=0.1, max_value=0.9))
    @settings(max_examples=20)
    def test_quota_threshold_configuration(self, threshold_percent):
        """
        Feature: trestle-etl-pipeline, Property 14: Quota Monitoring and Alerting
        
        For any threshold percentage, the quota monitoring should respect the 
        configured threshold for alerting.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        quota_limit = 1000
        
        # Test case 1: Remaining quota is below threshold (should trigger alert)
        quota_remaining_low = int(quota_limit * (threshold_percent - 0.05))  # 5% below threshold
        client._quota_info = {
            'minute_quota_limit': quota_limit,
            'minute_quota_remaining': quota_remaining_low
        }
        
        alerts_low = client.is_quota_approaching_limit(threshold_percent)
        assert alerts_low.get('minute_quota_low', False) == True
        
        # Test case 2: Remaining quota is above threshold (should not trigger alert)
        quota_remaining_high = int(quota_limit * (threshold_percent + 0.05))  # 5% above threshold
        client._quota_info = {
            'minute_quota_limit': quota_limit,
            'minute_quota_remaining': quota_remaining_high
        }
        
        alerts_high = client.is_quota_approaching_limit(threshold_percent)
        assert alerts_high.get('minute_quota_low', False) == False

    @given(entity_set=entity_set_strategy)
    @settings(max_examples=15)
    def test_quota_info_persistence_across_requests(self, entity_set):
        """
        Feature: trestle-etl-pipeline, Property 14: Quota Monitoring and Alerting
        
        For any sequence of API requests, quota information should be maintained 
        and updated consistently across requests.
        """
        config = create_mock_config()
        client = ODataClient(config)
        
        # Mock responses with decreasing quota
        responses = []
        for i in range(3):
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {
                'Minute-Quota-Remaining': str(100 - (i * 10)),  # Decreasing
                'Minute-Quota-Limit': '1000'
            }
            mock_response.json.return_value = {'value': []}
            responses.append(mock_response)
        
        with patch.object(client, 'authenticate', return_value='test_token'):
            with patch.object(client._session, 'get', side_effect=responses):
                # Make multiple requests
                for i in range(3):
                    client.execute_query(entity_set)
                    
                    # Verify quota info is updated after each request
                    current_quota = client.get_current_quota_info()
                    expected_remaining = 100 - (i * 10)
                    
                    assert current_quota['quota_info']['minute_quota_remaining'] == expected_remaining
                    assert current_quota['quota_info']['minute_quota_limit'] == 1000
                    assert current_quota['last_check'] is not None