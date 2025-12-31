"""
Property-based tests for Incremental Sync Manager.

Feature: trestle-etl-pipeline
Tests Properties 13, 15 for incremental updates and request batching.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from unittest.mock import Mock, MagicMock, patch

import pytest
from hypothesis import given, strategies as st, settings, assume

from trestle_etl.config import APIConfig, DatabaseConfig
from trestle_etl.odata_client import ODataClient
from trestle_etl.mysql_loader import MySQLLoader, SyncRun, SyncStatus
from trestle_etl.incremental_sync import (
    IncrementalSyncManager, IncrementalSyncError, DataType,
    BatchRequest, IncrementalSyncResult, BatchSyncResult
)


def create_mock_api_config() -> APIConfig:
    """Create a mock API configuration."""
    return APIConfig(
        client_id="test_id",
        client_secret="test_secret",
        base_url="https://api-test.example.com/odata",
        token_url="https://auth-test.example.com/token",
        timeout=30
    )


def create_mock_db_config() -> DatabaseConfig:
    """Create a mock database configuration."""
    return DatabaseConfig(
        host="localhost",
        port=3306,
        database="test_db",
        user="test_user",
        password="test_pass",
        charset="utf8mb4"
    )


# Strategy for generating timestamps
timestamp_strategy = st.datetimes(
    min_value=datetime(2020, 1, 1),
    max_value=datetime(2025, 12, 31)
)

# Strategy for generating data types
data_type_strategy = st.sampled_from([
    DataType.PROPERTY, DataType.MEDIA, DataType.MEMBER, DataType.OFFICE
])

# Strategy for generating lists of data types
data_types_list_strategy = st.lists(
    data_type_strategy,
    min_size=1,
    max_size=4,
    unique=True
)

# Strategy for generating filter expressions
filter_expr_strategy = st.one_of(
    st.none(),
    st.text(min_size=1, max_size=100)
)


class TestIncrementalUpdateEfficiency:
    """
    Property 13: Incremental Update Efficiency
    
    For any ETL run after the initial load, only records modified since the 
    last successful sync timestamp should be requested from the API.
    
    Validates: Requirements 4.2, 4.3
    """
    
    @given(
        last_sync_timestamp=timestamp_strategy,
        additional_filter=filter_expr_strategy
    )
    @settings(max_examples=100)
    def test_incremental_filter_includes_timestamp(
        self, last_sync_timestamp, additional_filter
    ):
        """
        Feature: trestle-etl-pipeline, Property 13: Incremental Update Efficiency
        
        For any last sync timestamp, the incremental filter should include
        a ModificationTimestamp filter to only fetch changed records.
        """
        api_config = create_mock_api_config()
        db_config = create_mock_db_config()
        
        mock_odata_client = Mock(spec=ODataClient)
        mock_mysql_loader = Mock(spec=MySQLLoader)
        
        manager = IncrementalSyncManager(
            odata_client=mock_odata_client,
            mysql_loader=mock_mysql_loader
        )
        
        # Build incremental filter
        filter_expr = manager.build_incremental_filter(
            last_sync_timestamp=last_sync_timestamp,
            additional_filter=additional_filter
        )
        
        # Verify filter includes timestamp condition
        expected_timestamp = last_sync_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        assert f"ModificationTimestamp gt {expected_timestamp}" in filter_expr
        
        # Verify additional filter is included if provided
        if additional_filter:
            assert additional_filter in filter_expr
            assert " and " in filter_expr
    
    @given(additional_filter=filter_expr_strategy)
    @settings(max_examples=50)
    def test_no_timestamp_filter_for_full_sync(self, additional_filter):
        """
        Feature: trestle-etl-pipeline, Property 13: Incremental Update Efficiency
        
        For any full sync (no last timestamp), the filter should not include
        a ModificationTimestamp condition.
        """
        api_config = create_mock_api_config()
        db_config = create_mock_db_config()
        
        mock_odata_client = Mock(spec=ODataClient)
        mock_mysql_loader = Mock(spec=MySQLLoader)
        
        manager = IncrementalSyncManager(
            odata_client=mock_odata_client,
            mysql_loader=mock_mysql_loader
        )
        
        # Build filter without timestamp
        filter_expr = manager.build_incremental_filter(
            last_sync_timestamp=None,
            additional_filter=additional_filter
        )
        
        # Verify no timestamp filter
        assert "ModificationTimestamp" not in filter_expr
        
        # Verify additional filter is returned as-is
        if additional_filter:
            assert filter_expr == additional_filter
        else:
            assert filter_expr == ""
    
    @given(last_sync_timestamp=timestamp_strategy)
    @settings(max_examples=100)
    def test_incremental_sync_uses_last_timestamp_from_metadata(
        self, last_sync_timestamp
    ):
        """
        Feature: trestle-etl-pipeline, Property 13: Incremental Update Efficiency
        
        For any incremental sync, the system should query the last successful
        sync timestamp from metadata and use it for filtering.
        """
        api_config = create_mock_api_config()
        db_config = create_mock_db_config()
        
        mock_odata_client = Mock(spec=ODataClient)
        mock_odata_client.execute_paginated_query.return_value = []
        
        mock_mysql_loader = Mock(spec=MySQLLoader)
        mock_mysql_loader.get_last_sync_timestamp.return_value = last_sync_timestamp
        
        manager = IncrementalSyncManager(
            odata_client=mock_odata_client,
            mysql_loader=mock_mysql_loader
        )
        
        # Execute batched sync with incremental
        manager.execute_batched_sync(
            data_types=[DataType.PROPERTY],
            use_incremental=True
        )
        
        # Verify last sync timestamp was queried
        mock_mysql_loader.get_last_sync_timestamp.assert_called_once()
        
        # Verify the filter used in the query includes the timestamp
        call_args = mock_odata_client.execute_paginated_query.call_args
        filter_expr = call_args.kwargs.get('filter_expr') or call_args[1].get('filter_expr')
        
        if filter_expr:
            expected_timestamp = last_sync_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
            assert expected_timestamp in filter_expr
    
    @given(
        timestamps=st.lists(timestamp_strategy, min_size=2, max_size=5)
    )
    @settings(max_examples=50)
    def test_incremental_filter_timestamp_format_is_odata_compliant(
        self, timestamps
    ):
        """
        Feature: trestle-etl-pipeline, Property 13: Incremental Update Efficiency
        
        For any timestamp, the OData filter should use ISO 8601 format
        compatible with OData protocol.
        """
        api_config = create_mock_api_config()
        db_config = create_mock_db_config()
        
        mock_odata_client = Mock(spec=ODataClient)
        mock_mysql_loader = Mock(spec=MySQLLoader)
        
        manager = IncrementalSyncManager(
            odata_client=mock_odata_client,
            mysql_loader=mock_mysql_loader
        )
        
        for timestamp in timestamps:
            filter_expr = manager.build_incremental_filter(
                last_sync_timestamp=timestamp
            )
            
            # Verify OData-compliant format: YYYY-MM-DDTHH:MM:SSZ
            import re
            pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z'
            assert re.search(pattern, filter_expr), \
                f"Filter should contain OData-compliant timestamp format: {filter_expr}"
    
    @given(
        last_sync_timestamp=timestamp_strategy,
        data_type=data_type_strategy
    )
    @settings(max_examples=100)
    def test_incremental_sync_only_fetches_modified_records(
        self, last_sync_timestamp, data_type
    ):
        """
        Feature: trestle-etl-pipeline, Property 13: Incremental Update Efficiency
        
        For any incremental sync, the API query should only request records
        modified after the last sync timestamp.
        """
        api_config = create_mock_api_config()
        db_config = create_mock_db_config()
        
        mock_odata_client = Mock(spec=ODataClient)
        mock_odata_client.execute_paginated_query.return_value = []
        
        mock_mysql_loader = Mock(spec=MySQLLoader)
        
        manager = IncrementalSyncManager(
            odata_client=mock_odata_client,
            mysql_loader=mock_mysql_loader
        )
        
        # Execute incremental sync
        result = manager.execute_incremental_sync(
            data_type=data_type,
            last_sync_timestamp=last_sync_timestamp
        )
        
        # Verify the query was made with correct filter
        mock_odata_client.execute_paginated_query.assert_called_once()
        call_args = mock_odata_client.execute_paginated_query.call_args
        
        # Check filter expression
        filter_expr = call_args.kwargs.get('filter_expr') or call_args[1].get('filter_expr')
        expected_timestamp = last_sync_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        assert filter_expr is not None
        assert f"ModificationTimestamp gt {expected_timestamp}" in filter_expr


class TestRequestBatchingEfficiency:
    """
    Property 15: Request Batching Efficiency
    
    For any ETL scenario requiring multiple data types, the number of API calls 
    should be minimized through efficient request batching.
    
    Validates: Requirements 4.5
    """
    
    @given(data_types=data_types_list_strategy)
    @settings(max_examples=100)
    def test_batch_requests_created_for_all_data_types(self, data_types):
        """
        Feature: trestle-etl-pipeline, Property 15: Request Batching Efficiency
        
        For any list of data types, batch requests should be created for each
        data type efficiently.
        """
        api_config = create_mock_api_config()
        db_config = create_mock_db_config()
        
        mock_odata_client = Mock(spec=ODataClient)
        mock_mysql_loader = Mock(spec=MySQLLoader)
        
        manager = IncrementalSyncManager(
            odata_client=mock_odata_client,
            mysql_loader=mock_mysql_loader
        )
        
        # Create batch requests
        requests = manager.create_batch_requests(
            data_types=data_types,
            last_sync_timestamp=None
        )
        
        # Verify one request per data type
        assert len(requests) == len(data_types)
        
        # Verify all data types are represented
        request_types = {r.data_type for r in requests}
        assert request_types == set(data_types)
    
    @given(
        data_types=data_types_list_strategy,
        last_sync_timestamp=st.one_of(st.none(), timestamp_strategy)
    )
    @settings(max_examples=100)
    def test_batch_requests_ordered_by_priority(
        self, data_types, last_sync_timestamp
    ):
        """
        Feature: trestle-etl-pipeline, Property 15: Request Batching Efficiency
        
        For any list of data types, batch requests should be ordered by priority
        to optimize processing order.
        """
        api_config = create_mock_api_config()
        db_config = create_mock_db_config()
        
        mock_odata_client = Mock(spec=ODataClient)
        mock_mysql_loader = Mock(spec=MySQLLoader)
        
        manager = IncrementalSyncManager(
            odata_client=mock_odata_client,
            mysql_loader=mock_mysql_loader
        )
        
        # Create batch requests
        requests = manager.create_batch_requests(
            data_types=data_types,
            last_sync_timestamp=last_sync_timestamp
        )
        
        # Verify requests are sorted by priority
        priorities = [r.priority for r in requests]
        assert priorities == sorted(priorities), \
            "Batch requests should be sorted by priority"
    
    @given(data_types=data_types_list_strategy)
    @settings(max_examples=100)
    def test_batched_sync_minimizes_api_calls(self, data_types):
        """
        Feature: trestle-etl-pipeline, Property 15: Request Batching Efficiency
        
        For any batched sync, the total API calls should be minimized by
        processing data types efficiently.
        """
        api_config = create_mock_api_config()
        db_config = create_mock_db_config()
        
        mock_odata_client = Mock(spec=ODataClient)
        mock_odata_client.execute_paginated_query.return_value = []
        
        mock_mysql_loader = Mock(spec=MySQLLoader)
        mock_mysql_loader.get_last_sync_timestamp.return_value = None
        
        manager = IncrementalSyncManager(
            odata_client=mock_odata_client,
            mysql_loader=mock_mysql_loader
        )
        
        # Execute batched sync
        result = manager.execute_batched_sync(
            data_types=data_types,
            use_incremental=False
        )
        
        # Verify API calls match number of data types (one call per type minimum)
        assert mock_odata_client.execute_paginated_query.call_count == len(data_types)
        
        # Verify all data types were processed
        assert len(result.results) == len(data_types)
    
    @given(
        data_types=data_types_list_strategy,
        custom_filters=st.dictionaries(
            keys=data_type_strategy,
            values=st.text(min_size=1, max_size=50),
            min_size=0,
            max_size=4
        )
    )
    @settings(max_examples=50)
    def test_batch_requests_include_custom_filters(
        self, data_types, custom_filters
    ):
        """
        Feature: trestle-etl-pipeline, Property 15: Request Batching Efficiency
        
        For any batch request with custom filters, the filters should be
        included in the request.
        """
        api_config = create_mock_api_config()
        db_config = create_mock_db_config()
        
        mock_odata_client = Mock(spec=ODataClient)
        mock_mysql_loader = Mock(spec=MySQLLoader)
        
        manager = IncrementalSyncManager(
            odata_client=mock_odata_client,
            mysql_loader=mock_mysql_loader
        )
        
        # Create batch requests with custom filters
        requests = manager.create_batch_requests(
            data_types=data_types,
            last_sync_timestamp=None,
            custom_filters=custom_filters
        )
        
        # Verify custom filters are included
        for request in requests:
            if request.data_type in custom_filters:
                expected_filter = custom_filters[request.data_type]
                assert expected_filter in request.filter_expr, \
                    f"Custom filter should be included for {request.data_type}"
    
    @given(
        data_types=data_types_list_strategy,
        last_sync_timestamp=timestamp_strategy
    )
    @settings(max_examples=100)
    def test_batched_sync_tracks_total_api_calls(
        self, data_types, last_sync_timestamp
    ):
        """
        Feature: trestle-etl-pipeline, Property 15: Request Batching Efficiency
        
        For any batched sync, the total API calls should be accurately tracked
        across all data types.
        """
        api_config = create_mock_api_config()
        db_config = create_mock_db_config()
        
        mock_odata_client = Mock(spec=ODataClient)
        mock_odata_client.execute_paginated_query.return_value = []
        
        mock_mysql_loader = Mock(spec=MySQLLoader)
        mock_mysql_loader.get_last_sync_timestamp.return_value = last_sync_timestamp
        
        manager = IncrementalSyncManager(
            odata_client=mock_odata_client,
            mysql_loader=mock_mysql_loader
        )
        
        # Execute batched sync
        result = manager.execute_batched_sync(
            data_types=data_types,
            use_incremental=True
        )
        
        # Verify total API calls is tracked
        assert result.total_api_calls >= len(data_types), \
            "Total API calls should be at least one per data type"
    
    @given(data_types=data_types_list_strategy)
    @settings(max_examples=50)
    def test_batched_sync_records_duration(self, data_types):
        """
        Feature: trestle-etl-pipeline, Property 15: Request Batching Efficiency
        
        For any batched sync, the duration should be recorded accurately.
        """
        api_config = create_mock_api_config()
        db_config = create_mock_db_config()
        
        mock_odata_client = Mock(spec=ODataClient)
        mock_odata_client.execute_paginated_query.return_value = []
        
        mock_mysql_loader = Mock(spec=MySQLLoader)
        mock_mysql_loader.get_last_sync_timestamp.return_value = None
        
        manager = IncrementalSyncManager(
            odata_client=mock_odata_client,
            mysql_loader=mock_mysql_loader
        )
        
        # Execute batched sync
        result = manager.execute_batched_sync(
            data_types=data_types,
            use_incremental=False
        )
        
        # Verify duration is recorded
        assert result.start_time is not None
        assert result.end_time is not None
        assert result.end_time >= result.start_time
        assert result.duration_seconds >= 0
    
    @given(
        estimated_records=st.integers(min_value=1, max_value=10000),
        quota_remaining=st.integers(min_value=1, max_value=1000),
        max_batch_size=st.integers(min_value=100, max_value=1000)
    )
    @settings(max_examples=100)
    def test_optimal_batch_size_calculation(
        self, estimated_records, quota_remaining, max_batch_size
    ):
        """
        Feature: trestle-etl-pipeline, Property 15: Request Batching Efficiency
        
        For any combination of estimated records and quota, the optimal batch
        size should minimize API calls while respecting quota limits.
        """
        api_config = create_mock_api_config()
        db_config = create_mock_db_config()
        
        mock_odata_client = Mock(spec=ODataClient)
        mock_mysql_loader = Mock(spec=MySQLLoader)
        
        manager = IncrementalSyncManager(
            odata_client=mock_odata_client,
            mysql_loader=mock_mysql_loader
        )
        
        # Calculate optimal batch size
        optimal_size = manager.calculate_optimal_batch_size(
            estimated_records=estimated_records,
            quota_remaining=quota_remaining,
            max_batch_size=max_batch_size
        )
        
        # Verify batch size is within bounds
        assert 0 < optimal_size <= max_batch_size, \
            f"Batch size {optimal_size} should be between 1 and {max_batch_size}"
        
        # Verify pages needed fits within quota
        pages_needed = (estimated_records + optimal_size - 1) // optimal_size
        assert pages_needed <= quota_remaining or optimal_size == max_batch_size, \
            f"Pages needed ({pages_needed}) should fit within quota ({quota_remaining})"
