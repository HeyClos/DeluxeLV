"""
Incremental Sync Manager for Trestle ETL Pipeline.

Handles incremental updates based on ModificationTimestamp, request batching
for multiple data types, and cost optimization strategies.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

from .odata_client import ODataClient
from .mysql_loader import MySQLLoader, SyncRun, SyncStatus


class IncrementalSyncError(Exception):
    """Raised when incremental sync operations fail."""
    pass


class DataType(Enum):
    """Supported data types for ETL operations."""
    PROPERTY = "Property"
    MEDIA = "Media"
    MEMBER = "Member"
    OFFICE = "Office"


@dataclass
class BatchRequest:
    """Represents a batched API request."""
    data_type: DataType
    filter_expr: str
    select_fields: Optional[List[str]] = None
    priority: int = 0  # Lower number = higher priority


@dataclass
class IncrementalSyncResult:
    """Result of an incremental sync operation."""
    data_type: DataType
    records_fetched: int = 0
    records_processed: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    api_calls_made: int = 0
    last_modification_timestamp: Optional[datetime] = None
    errors: List[str] = field(default_factory=list)
    success: bool = True


@dataclass
class BatchSyncResult:
    """Result of a batched sync operation across multiple data types."""
    results: Dict[DataType, IncrementalSyncResult] = field(default_factory=dict)
    total_api_calls: int = 0
    total_records_processed: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    @property
    def duration_seconds(self) -> float:
        """Calculate duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    @property
    def all_successful(self) -> bool:
        """Check if all data type syncs were successful."""
        return all(r.success for r in self.results.values())


class IncrementalSyncManager:
    """
    Manages incremental synchronization for the Trestle ETL pipeline.
    
    Handles:
    - Querying last successful sync timestamp from metadata
    - Building OData filters for ModificationTimestamp-based incremental updates
    - Request batching for multiple data types
    - Cost-optimized API usage
    """
    
    # Default fields to select for each data type
    DEFAULT_SELECT_FIELDS = {
        DataType.PROPERTY: [
            'ListingKey', 'ListPrice', 'PropertyType', 'BedroomsTotal',
            'BathroomsTotalInteger', 'LivingArea', 'LotSizeAcres', 'YearBuilt',
            'StandardStatus', 'ModificationTimestamp', 'StreetNumber',
            'StreetName', 'City', 'StateOrProvince', 'PostalCode'
        ],
        DataType.MEDIA: [
            'MediaKey', 'ResourceRecordKey', 'MediaURL', 'MediaType',
            'Order', 'ModificationTimestamp'
        ],
        DataType.MEMBER: [
            'MemberKey', 'MemberFirstName', 'MemberLastName', 'MemberEmail',
            'ModificationTimestamp'
        ],
        DataType.OFFICE: [
            'OfficeKey', 'OfficeName', 'OfficePhone', 'OfficeEmail',
            'ModificationTimestamp'
        ]
    }
    
    def __init__(
        self,
        odata_client: ODataClient,
        mysql_loader: MySQLLoader,
        incremental_field: str = "ModificationTimestamp",
        page_size: int = 1000,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize IncrementalSyncManager.
        
        Args:
            odata_client: OData client for API communication.
            mysql_loader: MySQL loader for database operations.
            incremental_field: Field name for incremental updates.
            page_size: Number of records per API request.
            logger: Optional logger instance.
        """
        self.odata_client = odata_client
        self.mysql_loader = mysql_loader
        self.incremental_field = incremental_field
        self.page_size = page_size
        self.logger = logger or logging.getLogger(__name__)
        self._api_calls_count = 0
    
    def get_last_sync_timestamp(self) -> Optional[datetime]:
        """
        Query the last successful sync timestamp from metadata.
        
        Returns:
            Last sync timestamp or None if no previous sync.
        """
        try:
            return self.mysql_loader.get_last_sync_timestamp()
        except Exception as e:
            self.logger.warning(f"Failed to get last sync timestamp: {e}")
            return None
    
    def build_incremental_filter(
        self,
        last_sync_timestamp: Optional[datetime],
        additional_filter: Optional[str] = None
    ) -> str:
        """
        Build OData filter expression for incremental updates.
        
        Args:
            last_sync_timestamp: Timestamp of last successful sync.
            additional_filter: Optional additional filter to combine.
            
        Returns:
            OData filter expression string.
        """
        filters = []
        
        if last_sync_timestamp:
            # Format timestamp for OData: 2024-01-15T10:30:00Z
            timestamp_str = last_sync_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
            filters.append(f"{self.incremental_field} gt {timestamp_str}")
        
        if additional_filter:
            filters.append(additional_filter)
        
        if filters:
            return " and ".join(filters)
        return ""
    
    def create_batch_requests(
        self,
        data_types: List[DataType],
        last_sync_timestamp: Optional[datetime] = None,
        custom_filters: Optional[Dict[DataType, str]] = None
    ) -> List[BatchRequest]:
        """
        Create batched requests for multiple data types.
        
        Args:
            data_types: List of data types to sync.
            last_sync_timestamp: Timestamp for incremental filter.
            custom_filters: Optional custom filters per data type.
            
        Returns:
            List of BatchRequest objects ordered by priority.
        """
        custom_filters = custom_filters or {}
        requests = []
        
        # Priority order: Property (0), Media (1), Member (2), Office (3)
        priority_map = {
            DataType.PROPERTY: 0,
            DataType.MEDIA: 1,
            DataType.MEMBER: 2,
            DataType.OFFICE: 3
        }
        
        for data_type in data_types:
            additional_filter = custom_filters.get(data_type)
            filter_expr = self.build_incremental_filter(
                last_sync_timestamp, 
                additional_filter
            )
            
            request = BatchRequest(
                data_type=data_type,
                filter_expr=filter_expr,
                select_fields=self.DEFAULT_SELECT_FIELDS.get(data_type),
                priority=priority_map.get(data_type, 99)
            )
            requests.append(request)
        
        # Sort by priority
        requests.sort(key=lambda r: r.priority)
        return requests

    def execute_incremental_sync(
        self,
        data_type: DataType,
        last_sync_timestamp: Optional[datetime] = None,
        additional_filter: Optional[str] = None,
        select_fields: Optional[List[str]] = None
    ) -> IncrementalSyncResult:
        """
        Execute incremental sync for a single data type.
        
        Args:
            data_type: The data type to sync.
            last_sync_timestamp: Timestamp for incremental filter.
            additional_filter: Optional additional filter expression.
            select_fields: Optional list of fields to select.
            
        Returns:
            IncrementalSyncResult with sync statistics.
        """
        result = IncrementalSyncResult(data_type=data_type)
        
        try:
            # Build filter expression
            filter_expr = self.build_incremental_filter(
                last_sync_timestamp, 
                additional_filter
            )
            
            # Use default fields if not specified
            if select_fields is None:
                select_fields = self.DEFAULT_SELECT_FIELDS.get(data_type)
            
            self.logger.info(
                f"Starting incremental sync for {data_type.value} "
                f"with filter: {filter_expr or 'None (full sync)'}"
            )
            
            # Execute paginated query
            records = self.odata_client.execute_paginated_query(
                entity_set=data_type.value,
                filter_expr=filter_expr if filter_expr else None,
                select_fields=select_fields,
                top=self.page_size
            )
            
            result.records_fetched = len(records)
            self._api_calls_count += 1  # At least one call
            
            # Track the latest modification timestamp
            if records:
                for record in records:
                    mod_ts = record.get(self.incremental_field)
                    if mod_ts:
                        if isinstance(mod_ts, str):
                            try:
                                mod_ts = datetime.fromisoformat(
                                    mod_ts.replace('Z', '+00:00')
                                )
                            except ValueError:
                                continue
                        if result.last_modification_timestamp is None or \
                           mod_ts > result.last_modification_timestamp:
                            result.last_modification_timestamp = mod_ts
            
            result.api_calls_made = self._api_calls_count
            result.records_processed = result.records_fetched
            
            self.logger.info(
                f"Incremental sync for {data_type.value} completed: "
                f"{result.records_fetched} records fetched"
            )
            
        except Exception as e:
            result.success = False
            result.errors.append(str(e))
            self.logger.error(f"Incremental sync failed for {data_type.value}: {e}")
        
        return result
    
    def execute_batched_sync(
        self,
        data_types: List[DataType],
        use_incremental: bool = True,
        custom_filters: Optional[Dict[DataType, str]] = None
    ) -> BatchSyncResult:
        """
        Execute batched sync for multiple data types.
        
        This method optimizes API usage by:
        - Using incremental updates when possible
        - Processing data types in priority order
        - Tracking total API calls across all types
        
        Args:
            data_types: List of data types to sync.
            use_incremental: Whether to use incremental updates.
            custom_filters: Optional custom filters per data type.
            
        Returns:
            BatchSyncResult with results for all data types.
        """
        batch_result = BatchSyncResult()
        batch_result.start_time = datetime.now()
        
        # Get last sync timestamp if using incremental
        last_sync_timestamp = None
        if use_incremental:
            last_sync_timestamp = self.get_last_sync_timestamp()
            if last_sync_timestamp:
                self.logger.info(
                    f"Using incremental sync from: {last_sync_timestamp}"
                )
            else:
                self.logger.info("No previous sync found, performing full sync")
        
        # Create batch requests
        requests = self.create_batch_requests(
            data_types=data_types,
            last_sync_timestamp=last_sync_timestamp,
            custom_filters=custom_filters
        )
        
        # Reset API call counter
        self._api_calls_count = 0
        
        # Execute each request
        for request in requests:
            self.logger.info(f"Processing batch request for {request.data_type.value}")
            
            result = self.execute_incremental_sync(
                data_type=request.data_type,
                last_sync_timestamp=last_sync_timestamp,
                additional_filter=custom_filters.get(request.data_type) if custom_filters else None,
                select_fields=request.select_fields
            )
            
            batch_result.results[request.data_type] = result
            batch_result.total_records_processed += result.records_processed
        
        batch_result.total_api_calls = self._api_calls_count
        batch_result.end_time = datetime.now()
        
        self.logger.info(
            f"Batched sync completed: {len(data_types)} data types, "
            f"{batch_result.total_api_calls} API calls, "
            f"{batch_result.total_records_processed} total records, "
            f"{batch_result.duration_seconds:.2f}s duration"
        )
        
        return batch_result
    
    def calculate_optimal_batch_size(
        self,
        estimated_records: int,
        quota_remaining: int,
        max_batch_size: int = 1000
    ) -> int:
        """
        Calculate optimal batch size based on quota and record count.
        
        Args:
            estimated_records: Estimated number of records to fetch.
            quota_remaining: Remaining API quota.
            max_batch_size: Maximum allowed batch size.
            
        Returns:
            Optimal batch size.
        """
        if quota_remaining <= 0:
            return 0
        
        # Calculate pages needed with max batch size
        pages_needed = (estimated_records + max_batch_size - 1) // max_batch_size
        
        # If we have enough quota, use max batch size
        if pages_needed <= quota_remaining:
            return max_batch_size
        
        # Otherwise, calculate batch size to fit within quota
        # We want: ceil(estimated_records / batch_size) <= quota_remaining
        # So: batch_size >= estimated_records / quota_remaining
        optimal_size = (estimated_records + quota_remaining - 1) // quota_remaining
        
        return min(optimal_size, max_batch_size)
    
    def should_use_incremental(
        self,
        last_sync_timestamp: Optional[datetime],
        max_age_hours: int = 24
    ) -> bool:
        """
        Determine if incremental sync should be used.
        
        Args:
            last_sync_timestamp: Timestamp of last sync.
            max_age_hours: Maximum age in hours for incremental sync.
            
        Returns:
            True if incremental sync should be used.
        """
        if last_sync_timestamp is None:
            return False
        
        age = datetime.now() - last_sync_timestamp
        return age.total_seconds() < (max_age_hours * 3600)
    
    def get_api_calls_count(self) -> int:
        """Get the current API calls count."""
        return self._api_calls_count
    
    def reset_api_calls_count(self) -> None:
        """Reset the API calls counter."""
        self._api_calls_count = 0
