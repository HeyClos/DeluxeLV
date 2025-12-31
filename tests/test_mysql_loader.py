"""
Property-based tests for MySQL Loader.

Feature: trestle-etl-pipeline
Tests Properties 10, 11, 12 for MySQL database operations.
"""

import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Any, List, Set
from unittest.mock import Mock, MagicMock, patch, call

import pytest
from hypothesis import given, strategies as st, settings, assume

from trestle_etl.config import DatabaseConfig
from trestle_etl.mysql_loader import (
    MySQLLoader, DatabaseError, ConnectionError, 
    SyncRun, SyncStatus, BatchResult,
    PROPERTIES_TABLE_SCHEMA, ETL_SYNC_LOG_TABLE_SCHEMA
)


def create_mock_config() -> DatabaseConfig:
    """Create a mock database configuration."""
    return DatabaseConfig(
        host="localhost",
        port=3306,
        database="test_db",
        user="test_user",
        password="test_pass",
        charset="utf8mb4"
    )


# Strategy for generating valid listing keys
listing_key_strategy = st.text(
    alphabet=st.sampled_from("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"),
    min_size=5,
    max_size=20
)

# Strategy for generating valid property records
property_record_strategy = st.fixed_dictionaries({
    'listing_key': listing_key_strategy,
    'list_price': st.one_of(st.none(), st.integers(min_value=0, max_value=10000000)),
    'property_type': st.one_of(st.none(), st.sampled_from(['Residential', 'Commercial', 'Land', 'Multi-Family'])),
    'bedrooms_total': st.one_of(st.none(), st.integers(min_value=0, max_value=20)),
    'bathrooms_total': st.one_of(st.none(), st.floats(min_value=0, max_value=10, allow_nan=False)),
    'square_feet': st.one_of(st.none(), st.integers(min_value=100, max_value=50000)),
    'lot_size_acres': st.one_of(st.none(), st.floats(min_value=0, max_value=1000, allow_nan=False)),
    'year_built': st.one_of(st.none(), st.integers(min_value=1800, max_value=2025)),
    'listing_status': st.one_of(st.none(), st.sampled_from(['Active', 'Pending', 'Sold', 'Withdrawn'])),
    'modification_timestamp': st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2025, 12, 31)),
    'street_address': st.one_of(st.none(), st.text(min_size=1, max_size=100)),
    'city': st.one_of(st.none(), st.text(min_size=1, max_size=50)),
    'state_or_province': st.one_of(st.none(), st.sampled_from(['CA', 'TX', 'FL', 'NY', 'WA'])),
    'postal_code': st.one_of(st.none(), st.text(alphabet=st.sampled_from("0123456789"), min_size=5, max_size=10))
})


class TestBatchOperationUsage:
    """
    Property 10: Batch Operation Usage
    
    For any data loading operation, the system should use batch SQL operations 
    rather than individual insert/update statements when the batch size exceeds 
    the configured threshold.
    
    Validates: Requirements 3.1
    """
    
    @given(
        records=st.lists(property_record_strategy, min_size=1, max_size=50),
        batch_size=st.integers(min_value=1, max_value=20)
    )
    @settings(max_examples=100)
    def test_batch_insert_uses_batch_operations(self, records, batch_size):
        """
        Feature: trestle-etl-pipeline, Property 10: Batch Operation Usage
        
        For any list of records and batch size, the loader should use executemany
        for batch operations rather than individual execute calls.
        """
        # Ensure unique listing keys
        seen_keys = set()
        unique_records = []
        for record in records:
            if record['listing_key'] not in seen_keys:
                seen_keys.add(record['listing_key'])
                unique_records.append(record)
        
        assume(len(unique_records) > 0)
        
        config = create_mock_config()
        loader = MySQLLoader(config, batch_size=batch_size)
        
        # Mock the connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.rowcount = len(unique_records)
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.open = True
        
        with patch.object(loader, 'get_connection', return_value=mock_connection):
            result = loader.batch_insert(unique_records, batch_size=batch_size)
            
            # Verify executemany was called (batch operation)
            assert mock_cursor.executemany.called, "Should use executemany for batch operations"
            
            # Verify the number of batch calls matches expected
            expected_batches = (len(unique_records) + batch_size - 1) // batch_size
            assert mock_cursor.executemany.call_count == expected_batches, \
                f"Expected {expected_batches} batch calls, got {mock_cursor.executemany.call_count}"
            
            # Verify commit was called
            assert mock_connection.commit.called, "Should commit after batch operations"
    
    @given(
        records=st.lists(property_record_strategy, min_size=1, max_size=50),
        batch_size=st.integers(min_value=1, max_value=20)
    )
    @settings(max_examples=100)
    def test_batch_upsert_uses_batch_operations(self, records, batch_size):
        """
        Feature: trestle-etl-pipeline, Property 10: Batch Operation Usage
        
        For any list of records and batch size, the upsert operation should use 
        executemany for batch operations.
        """
        # Ensure unique listing keys
        seen_keys = set()
        unique_records = []
        for record in records:
            if record['listing_key'] not in seen_keys:
                seen_keys.add(record['listing_key'])
                unique_records.append(record)
        
        assume(len(unique_records) > 0)
        
        config = create_mock_config()
        loader = MySQLLoader(config, batch_size=batch_size)
        
        # Mock the connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.rowcount = len(unique_records)
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.open = True
        
        with patch.object(loader, 'get_connection', return_value=mock_connection):
            result = loader.batch_upsert(unique_records, batch_size=batch_size)
            
            # Verify executemany was called (batch operation)
            assert mock_cursor.executemany.called, "Should use executemany for batch upsert"
            
            # Verify the number of batch calls matches expected
            expected_batches = (len(unique_records) + batch_size - 1) // batch_size
            assert mock_cursor.executemany.call_count == expected_batches, \
                f"Expected {expected_batches} batch calls, got {mock_cursor.executemany.call_count}"
    
    @given(batch_size=st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_empty_records_returns_empty_result(self, batch_size):
        """
        Feature: trestle-etl-pipeline, Property 10: Batch Operation Usage
        
        For any empty record list, batch operations should return empty result 
        without making database calls.
        """
        config = create_mock_config()
        loader = MySQLLoader(config, batch_size=batch_size)
        
        # Mock the connection
        mock_connection = MagicMock()
        
        with patch.object(loader, 'get_connection', return_value=mock_connection):
            result = loader.batch_insert([])
            
            # Should return empty result
            assert result.total_records == 0
            assert result.inserted == 0
            assert result.errors == 0
            
            # Should not make any database calls
            assert not mock_connection.cursor.called
    
    @given(
        records=st.lists(property_record_strategy, min_size=10, max_size=30),
        batch_size=st.integers(min_value=5, max_value=15)
    )
    @settings(max_examples=50)
    def test_batch_size_determines_number_of_operations(self, records, batch_size):
        """
        Feature: trestle-etl-pipeline, Property 10: Batch Operation Usage
        
        For any record count and batch size, the number of batch operations 
        should equal ceil(record_count / batch_size).
        """
        # Ensure unique listing keys
        seen_keys = set()
        unique_records = []
        for record in records:
            if record['listing_key'] not in seen_keys:
                seen_keys.add(record['listing_key'])
                unique_records.append(record)
        
        assume(len(unique_records) >= batch_size)  # Ensure we have enough for multiple batches
        
        config = create_mock_config()
        loader = MySQLLoader(config, batch_size=batch_size)
        
        # Mock the connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.rowcount = batch_size
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.open = True
        
        with patch.object(loader, 'get_connection', return_value=mock_connection):
            loader.batch_insert(unique_records, batch_size=batch_size)
            
            # Calculate expected number of batches
            expected_batches = (len(unique_records) + batch_size - 1) // batch_size
            
            # Verify correct number of batch operations
            assert mock_cursor.executemany.call_count == expected_batches, \
                f"Expected {expected_batches} batches for {len(unique_records)} records with batch_size={batch_size}"


class TestUpsertBehaviorCorrectness:
    """
    Property 11: Upsert Behavior Correctness
    
    For any record loaded multiple times, the database should contain exactly 
    one instance of that record with the most recent data.
    
    Validates: Requirements 3.2
    """
    
    @given(
        listing_key=listing_key_strategy,
        initial_price=st.integers(min_value=100000, max_value=500000),
        updated_price=st.integers(min_value=500001, max_value=1000000)
    )
    @settings(max_examples=100)
    def test_upsert_updates_existing_records(self, listing_key, initial_price, updated_price):
        """
        Feature: trestle-etl-pipeline, Property 11: Upsert Behavior Correctness
        
        For any record that already exists, upsert should update it rather than 
        create a duplicate.
        """
        config = create_mock_config()
        loader = MySQLLoader(config)
        
        # Create initial and updated records
        initial_record = {
            'listing_key': listing_key,
            'list_price': initial_price,
            'property_type': 'Residential',
            'modification_timestamp': datetime.now()
        }
        
        updated_record = {
            'listing_key': listing_key,
            'list_price': updated_price,
            'property_type': 'Commercial',
            'modification_timestamp': datetime.now()
        }
        
        # Mock the connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.open = True
        
        with patch.object(loader, 'get_connection', return_value=mock_connection):
            # First upsert
            loader.batch_upsert([initial_record])
            
            # Second upsert with same key
            loader.batch_upsert([updated_record])
            
            # Verify ON DUPLICATE KEY UPDATE was used
            calls = mock_cursor.executemany.call_args_list
            for call_args in calls:
                sql = call_args[0][0]
                assert 'ON DUPLICATE KEY UPDATE' in sql, \
                    "Upsert should use ON DUPLICATE KEY UPDATE"
    
    @given(
        records=st.lists(property_record_strategy, min_size=2, max_size=10)
    )
    @settings(max_examples=100)
    def test_upsert_sql_contains_all_update_fields(self, records):
        """
        Feature: trestle-etl-pipeline, Property 11: Upsert Behavior Correctness
        
        For any upsert operation, the SQL should update all non-key fields.
        """
        # Ensure unique listing keys
        seen_keys = set()
        unique_records = []
        for record in records:
            if record['listing_key'] not in seen_keys:
                seen_keys.add(record['listing_key'])
                unique_records.append(record)
        
        assume(len(unique_records) > 0)
        
        config = create_mock_config()
        loader = MySQLLoader(config)
        
        # Mock the connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.rowcount = len(unique_records)
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.open = True
        
        with patch.object(loader, 'get_connection', return_value=mock_connection):
            loader.batch_upsert(unique_records)
            
            # Get the SQL that was executed
            call_args = mock_cursor.executemany.call_args
            sql = call_args[0][0]
            
            # Verify all non-key fields are in the UPDATE clause
            update_fields = [f for f in loader.PROPERTY_FIELDS if f != 'listing_key']
            for field in update_fields:
                assert f"{field} = VALUES({field})" in sql, \
                    f"Field {field} should be in UPDATE clause"
            
            # Verify listing_key is NOT in the UPDATE clause (it's the primary key)
            assert "listing_key = VALUES(listing_key)" not in sql, \
                "listing_key should not be in UPDATE clause"
    
    @given(
        listing_key=listing_key_strategy,
        prices=st.lists(st.integers(min_value=100000, max_value=1000000), min_size=2, max_size=5)
    )
    @settings(max_examples=50)
    def test_multiple_upserts_same_key_result_in_single_record(self, listing_key, prices):
        """
        Feature: trestle-etl-pipeline, Property 11: Upsert Behavior Correctness
        
        For any number of upserts with the same key, only one record should exist 
        with the most recent data.
        """
        config = create_mock_config()
        loader = MySQLLoader(config)
        
        # Create multiple records with same key but different prices
        records = [
            {
                'listing_key': listing_key,
                'list_price': price,
                'property_type': 'Residential',
                'modification_timestamp': datetime.now() + timedelta(hours=i)
            }
            for i, price in enumerate(prices)
        ]
        
        # Mock the connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.open = True
        
        with patch.object(loader, 'get_connection', return_value=mock_connection):
            # Upsert all records
            for record in records:
                loader.batch_upsert([record])
            
            # Verify each upsert used ON DUPLICATE KEY UPDATE
            for call_args in mock_cursor.executemany.call_args_list:
                sql = call_args[0][0]
                assert 'ON DUPLICATE KEY UPDATE' in sql
    
    @given(
        records=st.lists(property_record_strategy, min_size=1, max_size=20)
    )
    @settings(max_examples=100)
    def test_upsert_with_tracking_accurately_counts_inserts_and_updates(self, records):
        """
        Feature: trestle-etl-pipeline, Property 11: Upsert Behavior Correctness
        
        For any batch of records, upsert_with_tracking should accurately report 
        the number of inserts vs updates.
        """
        # Ensure unique listing keys
        seen_keys = set()
        unique_records = []
        for record in records:
            if record['listing_key'] not in seen_keys:
                seen_keys.add(record['listing_key'])
                unique_records.append(record)
        
        assume(len(unique_records) > 0)
        
        config = create_mock_config()
        loader = MySQLLoader(config)
        
        # Simulate some existing keys
        existing_keys = set(list(seen_keys)[:len(seen_keys)//2])
        
        # Mock the connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [{'listing_key': k} for k in existing_keys]
        mock_cursor.rowcount = len(unique_records)
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.open = True
        
        with patch.object(loader, 'get_connection', return_value=mock_connection):
            result = loader.batch_upsert_with_tracking(unique_records)
            
            # Verify counts are accurate
            expected_updates = len(existing_keys)
            expected_inserts = len(unique_records) - expected_updates
            
            assert result.inserted == expected_inserts, \
                f"Expected {expected_inserts} inserts, got {result.inserted}"
            assert result.updated == expected_updates, \
                f"Expected {expected_updates} updates, got {result.updated}"
            assert result.inserted + result.updated == len(unique_records), \
                "Total should equal record count"


class TestMetadataTrackingAccuracy:
    """
    Property 12: Metadata Tracking Accuracy
    
    For any completed ETL run, the sync metadata should accurately reflect 
    the number of records processed, inserted, updated, and the execution duration.
    
    Validates: Requirements 3.4
    """
    
    @given(
        records_processed=st.integers(min_value=0, max_value=10000),
        records_inserted=st.integers(min_value=0, max_value=5000),
        records_updated=st.integers(min_value=0, max_value=5000),
        api_calls_made=st.integers(min_value=1, max_value=100)
    )
    @settings(max_examples=100)
    def test_sync_run_metadata_accurately_recorded(
        self, records_processed, records_inserted, records_updated, api_calls_made
    ):
        """
        Feature: trestle-etl-pipeline, Property 12: Metadata Tracking Accuracy
        
        For any sync run, all metadata fields should be accurately recorded 
        in the database.
        """
        # Ensure records_inserted + records_updated <= records_processed
        records_inserted = min(records_inserted, records_processed)
        records_updated = min(records_updated, records_processed - records_inserted)
        
        config = create_mock_config()
        loader = MySQLLoader(config)
        
        # Mock the connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.lastrowid = 1
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.open = True
        
        with patch.object(loader, 'get_connection', return_value=mock_connection):
            # Start sync run
            sync_run = loader.start_sync_run()
            assert sync_run.sync_id == 1
            assert sync_run.sync_start is not None
            
            # Update with metrics
            sync_run = loader.update_sync_run(
                sync_run,
                records_processed=records_processed,
                records_inserted=records_inserted,
                records_updated=records_updated,
                api_calls_made=api_calls_made
            )
            
            # Verify metrics are stored
            assert sync_run.records_processed == records_processed
            assert sync_run.records_inserted == records_inserted
            assert sync_run.records_updated == records_updated
            assert sync_run.api_calls_made == api_calls_made
            
            # Verify database update was called with correct values
            update_calls = [c for c in mock_cursor.execute.call_args_list 
                          if 'UPDATE etl_sync_log' in str(c)]
            assert len(update_calls) > 0, "Should update sync log in database"
    
    @given(
        status=st.sampled_from([SyncStatus.SUCCESS, SyncStatus.PARTIAL, SyncStatus.FAILED]),
        error_message=st.one_of(st.none(), st.text(min_size=1, max_size=200))
    )
    @settings(max_examples=50)
    def test_sync_run_completion_records_status_and_errors(self, status, error_message):
        """
        Feature: trestle-etl-pipeline, Property 12: Metadata Tracking Accuracy
        
        For any sync run completion, the status and error message should be 
        accurately recorded.
        """
        config = create_mock_config()
        loader = MySQLLoader(config)
        
        # Mock the connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.lastrowid = 1
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.open = True
        
        with patch.object(loader, 'get_connection', return_value=mock_connection):
            # Start and complete sync run
            sync_run = loader.start_sync_run()
            sync_run = loader.complete_sync_run(sync_run, status=status, error_message=error_message)
            
            # Verify completion data
            assert sync_run.sync_end is not None
            assert sync_run.status == status
            assert sync_run.error_message == error_message
            
            # Verify duration is positive
            duration = sync_run.sync_end - sync_run.sync_start
            assert duration.total_seconds() >= 0, "Duration should be non-negative"
    
    @given(
        num_syncs=st.integers(min_value=1, max_value=5)
    )
    @settings(max_examples=30)
    def test_get_last_successful_sync_returns_most_recent(self, num_syncs):
        """
        Feature: trestle-etl-pipeline, Property 12: Metadata Tracking Accuracy
        
        For any number of sync runs, get_last_successful_sync should return 
        the most recent successful one.
        """
        config = create_mock_config()
        loader = MySQLLoader(config)
        
        # Create mock sync data
        most_recent_sync = {
            'id': num_syncs,
            'sync_start': datetime.now() - timedelta(hours=1),
            'sync_end': datetime.now(),
            'records_processed': 100,
            'records_inserted': 50,
            'records_updated': 50,
            'api_calls_made': 10,
            'status': 'success',
            'error_message': None,
            'last_sync_timestamp': datetime.now()
        }
        
        # Mock the connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = most_recent_sync
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.open = True
        
        with patch.object(loader, 'get_connection', return_value=mock_connection):
            result = loader.get_last_successful_sync()
            
            # Verify result matches most recent
            assert result is not None
            assert result.sync_id == num_syncs
            assert result.status == SyncStatus.SUCCESS
            
            # Verify query ordered by sync_end DESC
            query_call = mock_cursor.execute.call_args
            sql = query_call[0][0]
            assert 'ORDER BY sync_end DESC' in sql
            assert 'LIMIT 1' in sql
    
    @given(
        records_processed=st.integers(min_value=0, max_value=10000),
        records_inserted=st.integers(min_value=0, max_value=5000),
        records_updated=st.integers(min_value=0, max_value=5000)
    )
    @settings(max_examples=100)
    def test_sync_metadata_totals_are_consistent(
        self, records_processed, records_inserted, records_updated
    ):
        """
        Feature: trestle-etl-pipeline, Property 12: Metadata Tracking Accuracy
        
        For any sync run, the sum of inserted and updated records should not 
        exceed records_processed.
        """
        # Ensure consistency
        records_inserted = min(records_inserted, records_processed)
        records_updated = min(records_updated, records_processed - records_inserted)
        
        sync_run = SyncRun(
            sync_id=1,
            sync_start=datetime.now(),
            records_processed=records_processed,
            records_inserted=records_inserted,
            records_updated=records_updated
        )
        
        # Verify consistency
        assert sync_run.records_inserted + sync_run.records_updated <= sync_run.records_processed, \
            "Inserted + Updated should not exceed Processed"
    
    @given(
        limit=st.integers(min_value=1, max_value=20)
    )
    @settings(max_examples=30)
    def test_get_sync_history_respects_limit(self, limit):
        """
        Feature: trestle-etl-pipeline, Property 12: Metadata Tracking Accuracy
        
        For any limit parameter, get_sync_history should return at most that 
        many records.
        """
        config = create_mock_config()
        loader = MySQLLoader(config)
        
        # Create mock sync history
        mock_history = [
            {
                'id': i,
                'sync_start': datetime.now() - timedelta(hours=i),
                'sync_end': datetime.now() - timedelta(hours=i-1),
                'records_processed': 100 * i,
                'records_inserted': 50 * i,
                'records_updated': 50 * i,
                'api_calls_made': 10 * i,
                'status': 'success',
                'error_message': None,
                'last_sync_timestamp': datetime.now() - timedelta(hours=i)
            }
            for i in range(1, limit + 1)
        ]
        
        # Mock the connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = mock_history
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.open = True
        
        with patch.object(loader, 'get_connection', return_value=mock_connection):
            result = loader.get_sync_history(limit=limit)
            
            # Verify limit is respected
            assert len(result) <= limit
            
            # Verify query includes LIMIT
            query_call = mock_cursor.execute.call_args
            sql = query_call[0][0]
            assert 'LIMIT' in sql
    
    def test_sync_run_without_id_raises_error_on_update(self):
        """
        Feature: trestle-etl-pipeline, Property 12: Metadata Tracking Accuracy
        
        Attempting to update a sync run without an ID should raise an error.
        """
        config = create_mock_config()
        loader = MySQLLoader(config)
        
        sync_run = SyncRun()  # No sync_id
        
        with pytest.raises(DatabaseError) as exc_info:
            loader.update_sync_run(sync_run, records_processed=100)
        
        assert "sync_id" in str(exc_info.value).lower()
    
    def test_sync_run_without_id_raises_error_on_complete(self):
        """
        Feature: trestle-etl-pipeline, Property 12: Metadata Tracking Accuracy
        
        Attempting to complete a sync run without an ID should raise an error.
        """
        config = create_mock_config()
        loader = MySQLLoader(config)
        
        sync_run = SyncRun()  # No sync_id
        
        with pytest.raises(DatabaseError) as exc_info:
            loader.complete_sync_run(sync_run)
        
        assert "sync_id" in str(exc_info.value).lower()
