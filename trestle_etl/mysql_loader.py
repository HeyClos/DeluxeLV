"""
MySQL Loader for Trestle ETL Pipeline.

Handles database connection management, batch operations, and data loading
for the Trestle ETL pipeline.
"""

import logging
import time
import random
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from contextlib import contextmanager
from enum import Enum

import pymysql
from pymysql.cursors import DictCursor

from .config import DatabaseConfig


class DatabaseError(Exception):
    """Raised when database operations fail."""
    pass


class ConnectionError(Exception):
    """Raised when database connection fails."""
    pass


class SyncStatus(Enum):
    """Status of ETL sync run."""
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


@dataclass
class SyncRun:
    """Metadata for an ETL sync run."""
    sync_id: Optional[int] = None
    sync_start: Optional[datetime] = None
    sync_end: Optional[datetime] = None
    records_processed: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    api_calls_made: int = 0
    status: SyncStatus = SyncStatus.SUCCESS
    error_message: Optional[str] = None
    last_sync_timestamp: Optional[datetime] = None


@dataclass
class BatchResult:
    """Result of a batch operation."""
    total_records: int = 0
    inserted: int = 0
    updated: int = 0
    errors: int = 0
    error_messages: List[str] = field(default_factory=list)


# SQL schema definitions
PROPERTIES_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS properties (
    ListingKey VARCHAR(255) PRIMARY KEY,
    ListPrice DECIMAL(12,2),
    PropertyType VARCHAR(100),
    BedroomsTotal INT,
    BathroomsTotalInteger DECIMAL(3,1),
    LivingArea INT,
    LotSizeAcres DECIMAL(10,4),
    YearBuilt INT,
    StandardStatus VARCHAR(50),
    ModificationTimestamp DATETIME,
    StreetNumber VARCHAR(50),
    StreetName VARCHAR(200),
    City VARCHAR(100),
    StateOrProvince VARCHAR(50),
    PostalCode VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_ModificationTimestamp (ModificationTimestamp),
    INDEX idx_PropertyType (PropertyType),
    INDEX idx_StandardStatus (StandardStatus),
    INDEX idx_City (City),
    INDEX idx_PostalCode (PostalCode)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

ETL_SYNC_LOG_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS etl_sync_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sync_start DATETIME NOT NULL,
    sync_end DATETIME,
    records_processed INT DEFAULT 0,
    records_inserted INT DEFAULT 0,
    records_updated INT DEFAULT 0,
    api_calls_made INT DEFAULT 0,
    status ENUM('success', 'partial', 'failed') DEFAULT 'success',
    error_message TEXT,
    last_sync_timestamp DATETIME,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_sync_start (sync_start),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


class MySQLLoader:
    """
    MySQL database loader for Trestle ETL pipeline.
    
    Handles connection management with pooling and retry logic,
    batch insert/update operations, and sync metadata tracking.
    """
    
    # Fallback property fields for insert/update operations.
    # Used only when dynamic column detection is unavailable.
    # These match the API field names stored as-is in MySQL.
    PROPERTY_FIELDS = [
        'ListingKey', 'ListPrice', 'PropertyType', 'BedroomsTotal',
        'BathroomsTotalInteger', 'LivingArea', 'LotSizeAcres', 'YearBuilt',
        'StandardStatus', 'ModificationTimestamp', 'StreetNumber',
        'StreetName', 'City', 'StateOrProvince', 'PostalCode'
    ]
    
    def __init__(
        self,
        config: DatabaseConfig,
        batch_size: int = 1000,
        max_retries: int = 3,
        base_delay: float = 1.0,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize MySQLLoader.
        
        Args:
            config: Database configuration.
            batch_size: Default batch size for operations.
            max_retries: Maximum retry attempts for connection failures.
            base_delay: Base delay in seconds for exponential backoff.
            logger: Optional logger instance.
        """
        self.config = config
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.logger = logger or logging.getLogger(__name__)
        self._connection: Optional[pymysql.Connection] = None
        self._current_sync_run: Optional[SyncRun] = None
        self._db_columns: Optional[set] = None
    
    def _calculate_backoff_delay(self, attempt: int) -> float:
        """
        Calculate exponential backoff delay with jitter.
        
        Args:
            attempt: Current attempt number (0-based).
            
        Returns:
            Delay in seconds.
        """
        delay = self.base_delay * (2 ** attempt)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        return max(0, delay + jitter)
    
    def create_connection(self) -> pymysql.Connection:
        """
        Create database connection with retry logic.
        
        Returns:
            Active database connection.
            
        Raises:
            ConnectionError: If connection fails after all retries.
        """
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                connection = pymysql.connect(
                    host=self.config.host,
                    port=self.config.port,
                    user=self.config.user,
                    password=self.config.password,
                    database=self.config.database,
                    charset=self.config.charset,
                    cursorclass=DictCursor,
                    autocommit=False
                )
                self.logger.info(f"Database connection established to {self.config.host}:{self.config.port}")
                return connection
                
            except pymysql.Error as e:
                last_exception = e
                self.logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < self.max_retries:
                    delay = self._calculate_backoff_delay(attempt)
                    self.logger.info(f"Retrying in {delay:.2f} seconds...")
                    time.sleep(delay)
        
        raise ConnectionError(
            f"Failed to connect to database after {self.max_retries + 1} attempts: {str(last_exception)}"
        )
    
    def get_connection(self) -> pymysql.Connection:
        """
        Get active database connection, creating one if needed.
        
        Returns:
            Active database connection.
        """
        if self._connection is None or not self._connection.open:
            self._connection = self.create_connection()
        return self._connection
    
    @contextmanager
    def connection_context(self):
        """
        Context manager for database connection.
        
        Validates the connection is alive before yielding. If the connection
        is in a bad state, it reconnects automatically.
        
        Yields:
            Active database connection.
        """
        connection = self.get_connection()
        try:
            # Validate the connection is still alive
            connection.ping(reconnect=True)
            yield connection
        except pymysql.OperationalError:
            # Connection is dead — reconnect and retry
            self.logger.warning("Database connection lost, reconnecting...")
            self._connection = None
            connection = self.get_connection()
            yield connection
    
    def close_connection(self) -> None:
        """Close the database connection."""
        if self._connection and self._connection.open:
            self._connection.close()
            self._connection = None
            self.logger.info("Database connection closed")

    def _get_db_columns(self) -> set:
        """
        Get the set of actual column names in the properties table.
        
        Caches the result so the DB is only queried once per loader instance.
        
        Returns:
            Set of column names that exist in the properties table.
        """
        if self._db_columns is not None:
            return self._db_columns
        
        try:
            with self.connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'properties'",
                        (self.config.database,)
                    )
                    self._db_columns = {row['COLUMN_NAME'] for row in cursor.fetchall()}
                    self.logger.info(f"Detected {len(self._db_columns)} columns in properties table")
        except pymysql.Error as e:
            self.logger.warning(f"Could not detect DB columns, falling back to PROPERTY_FIELDS: {e}")
            self._db_columns = set(self.PROPERTY_FIELDS)
        
        return self._db_columns
    
    def initialize_schema(self) -> None:
        """
        Initialize database schema by creating required tables.
        
        Raises:
            DatabaseError: If schema creation fails.
        """
        try:
            with self.connection_context() as conn:
                with conn.cursor() as cursor:
                    # Create properties table
                    cursor.execute(PROPERTIES_TABLE_SCHEMA)
                    self.logger.info("Properties table created/verified")
                    
                    # Create ETL sync log table
                    cursor.execute(ETL_SYNC_LOG_TABLE_SCHEMA)
                    self.logger.info("ETL sync log table created/verified")
                    
                conn.commit()
                self.logger.info("Database schema initialized successfully")
                
        except pymysql.Error as e:
            raise DatabaseError(f"Failed to initialize schema: {str(e)}")

    def _prepare_record_values(self, record: Dict[str, Any], fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Prepare record values for database insertion.
        
        Args:
            record: Record dictionary.
            fields: Optional list of fields to prepare. Defaults to PROPERTY_FIELDS.
            
        Returns:
            Dictionary with prepared values.
        """
        prepared = {}
        target_fields = fields or self.PROPERTY_FIELDS
        
        for field in target_fields:
            value = record.get(field)
            
            # Handle special conversions
            if value is None:
                prepared[field] = None
            elif isinstance(value, Decimal):
                prepared[field] = float(value)
            elif isinstance(value, datetime):
                prepared[field] = value
            else:
                prepared[field] = value
        
        return prepared
    
    def batch_insert(
        self,
        records: List[Dict[str, Any]],
        batch_size: Optional[int] = None
    ) -> BatchResult:
        """
        Insert records in batches.
        
        Args:
            records: List of records to insert.
            batch_size: Optional batch size override.
            
        Returns:
            BatchResult with operation statistics.
            
        Raises:
            DatabaseError: If batch insert fails.
        """
        if not records:
            return BatchResult()
        
        batch_size = batch_size or self.batch_size
        result = BatchResult(total_records=len(records))
        
        # Determine fields dynamically from records and DB columns
        db_columns = self._get_db_columns()
        sample_fields = set()
        for record in records[:10]:
            sample_fields.update(record.keys())
        fields = sorted(f for f in sample_fields if f in db_columns and not f.startswith('_'))
        if 'ListingKey' in fields:
            fields.remove('ListingKey')
        fields = ['ListingKey'] + fields
        
        # Build INSERT statement
        fields_str = ', '.join(fields)
        placeholders = ', '.join(['%s'] * len(fields))
        insert_sql = f"INSERT INTO properties ({fields_str}) VALUES ({placeholders})"
        
        try:
            with self.connection_context() as conn:
                with conn.cursor() as cursor:
                    # Process in batches
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i + batch_size]
                        batch_values = []
                        
                        for record in batch:
                            prepared = self._prepare_record_values(record, fields)
                            values = tuple(prepared.get(f) for f in fields)
                            batch_values.append(values)
                        
                        try:
                            cursor.executemany(insert_sql, batch_values)
                            result.inserted += cursor.rowcount
                        except pymysql.IntegrityError as e:
                            # Handle duplicate key errors
                            result.errors += len(batch)
                            result.error_messages.append(f"Batch {i//batch_size}: {str(e)}")
                            self.logger.warning(f"Batch insert error: {str(e)}")
                    
                    conn.commit()
                    
        except pymysql.Error as e:
            raise DatabaseError(f"Batch insert failed: {str(e)}")
        
        return result
    
    def batch_upsert(
        self,
        records: List[Dict[str, Any]],
        batch_size: Optional[int] = None
    ) -> BatchResult:
        """
        Insert or update records in batches using ON DUPLICATE KEY UPDATE.
        
        Args:
            records: List of records to upsert.
            batch_size: Optional batch size override.
            
        Returns:
            BatchResult with operation statistics.
            
        Raises:
            DatabaseError: If batch upsert fails.
        """
        if not records:
            return BatchResult()
        
        batch_size = batch_size or self.batch_size
        result = BatchResult(total_records=len(records))
        
        # Determine fields dynamically from records and DB columns
        db_columns = self._get_db_columns()
        sample_fields = set()
        for record in records[:10]:
            sample_fields.update(record.keys())
        fields = sorted(f for f in sample_fields if f in db_columns and not f.startswith('_'))
        if 'ListingKey' in fields:
            fields.remove('ListingKey')
        fields = ['ListingKey'] + fields
        
        # Build UPSERT statement (INSERT ... ON DUPLICATE KEY UPDATE)
        fields_str = ', '.join(fields)
        placeholders = ', '.join(['%s'] * len(fields))
        
        # Update all fields except ListingKey on duplicate
        update_fields = [f for f in fields if f != 'ListingKey']
        update_clause = ', '.join([f"{f} = VALUES({f})" for f in update_fields])
        
        upsert_sql = f"""
            INSERT INTO properties ({fields_str}) 
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
        """
        
        try:
            with self.connection_context() as conn:
                with conn.cursor() as cursor:
                    # Process in batches
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i + batch_size]
                        batch_values = []
                        
                        for record in batch:
                            prepared = self._prepare_record_values(record, fields)
                            values = tuple(prepared.get(f) for f in fields)
                            batch_values.append(values)
                        
                        try:
                            cursor.executemany(upsert_sql, batch_values)
                            # rowcount: 1 for insert, 2 for update, 0 for no change
                            affected = cursor.rowcount
                            # Estimate inserts vs updates (rough approximation)
                            # In MySQL, affected_rows = 1 for insert, 2 for update
                            result.inserted += len(batch)  # Count all as processed
                            
                        except pymysql.Error as e:
                            result.errors += len(batch)
                            result.error_messages.append(f"Batch {i//batch_size}: {str(e)}")
                            self.logger.warning(f"Batch upsert error: {str(e)}")
                            # Continue with next batch
                            continue
                    
                    conn.commit()
                    
        except pymysql.Error as e:
            raise DatabaseError(f"Batch upsert failed: {str(e)}")
        
        return result
    
    def batch_upsert_with_tracking(
        self,
        records: List[Dict[str, Any]],
        batch_size: Optional[int] = None
    ) -> BatchResult:
        """
        Insert or update records with accurate insert/update tracking.
        
        This method checks for existing records to accurately track
        inserts vs updates.
        
        Args:
            records: List of records to upsert.
            batch_size: Optional batch size override.
            
        Returns:
            BatchResult with accurate insert/update counts.
            
        Raises:
            DatabaseError: If operation fails.
        """
        if not records:
            return BatchResult()
        
        batch_size = batch_size or self.batch_size
        result = BatchResult(total_records=len(records))
        
        # Determine fields dynamically from records and DB columns
        db_columns = self._get_db_columns()
        sample_fields = set()
        for record in records[:10]:
            sample_fields.update(record.keys())
        fields = sorted(f for f in sample_fields if f in db_columns and not f.startswith('_'))
        if 'ListingKey' in fields:
            fields.remove('ListingKey')
        fields = ['ListingKey'] + fields
        
        try:
            with self.connection_context() as conn:
                with conn.cursor() as cursor:
                    # Get existing listing keys
                    listing_keys = [r.get('ListingKey') for r in records if r.get('ListingKey')]
                    
                    if listing_keys:
                        placeholders = ', '.join(['%s'] * len(listing_keys))
                        cursor.execute(
                            f"SELECT ListingKey FROM properties WHERE ListingKey IN ({placeholders})",
                            listing_keys
                        )
                        existing_keys = {row['ListingKey'] for row in cursor.fetchall()}
                    else:
                        existing_keys = set()
                    
                    # Separate into inserts and updates
                    inserts = []
                    updates = []
                    
                    for record in records:
                        key = record.get('ListingKey')
                        if key in existing_keys:
                            updates.append(record)
                        else:
                            inserts.append(record)
                    
                    # Perform upsert
                    fields_str = ', '.join(fields)
                    placeholders = ', '.join(['%s'] * len(fields))
                    update_fields = [f for f in fields if f != 'ListingKey']
                    update_clause = ', '.join([f"{f} = VALUES({f})" for f in update_fields])
                    
                    upsert_sql = f"""
                        INSERT INTO properties ({fields_str}) 
                        VALUES ({placeholders})
                        ON DUPLICATE KEY UPDATE {update_clause}
                    """
                    
                    # Process in batches
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i + batch_size]
                        batch_values = []
                        
                        for record in batch:
                            prepared = self._prepare_record_values(record, fields)
                            values = tuple(prepared.get(f) for f in fields)
                            batch_values.append(values)
                        
                        try:
                            cursor.executemany(upsert_sql, batch_values)
                        except pymysql.Error as e:
                            result.errors += len(batch)
                            result.error_messages.append(f"Batch {i//batch_size}: {str(e)}")
                            self.logger.warning(f"Batch upsert error: {str(e)}")
                            continue
                    
                    conn.commit()
                    
                    # Set accurate counts
                    result.inserted = len(inserts)
                    result.updated = len(updates)
                    
        except pymysql.Error as e:
            raise DatabaseError(f"Batch upsert with tracking failed: {str(e)}")
        
        return result

    def batch_upsert_with_checkpoint(
        self,
        records: List[Dict[str, Any]],
        sync_run: Optional[SyncRun] = None,
        checkpoint_size: int = 5000,
        batch_size: Optional[int] = None,
        timestamp_field: str = 'ModificationTimestamp',
        on_checkpoint: Optional[Any] = None
    ) -> BatchResult:
        """
        Insert or update records with periodic checkpoint commits.
        
        Commits every `checkpoint_size` records and updates the sync run's
        last_sync_timestamp to the max timestamp seen so far. This ensures
        that if the process is interrupted, only the current checkpoint batch
        is lost and the next run can resume from the last committed watermark.
        
        Args:
            records: List of records to upsert.
            sync_run: Optional SyncRun to update at each checkpoint.
            checkpoint_size: Number of records between commits (default 5000).
            batch_size: SQL batch size for executemany (default self.batch_size).
            timestamp_field: Field name to track the watermark timestamp.
            on_checkpoint: Optional callback(checkpoint_num, total_committed, batch_result)
                          called after each checkpoint commit.
            
        Returns:
            BatchResult with cumulative insert/update counts.
            
        Raises:
            DatabaseError: If a checkpoint commit fails.
        """
        if not records:
            return BatchResult()
        
        batch_size = batch_size or self.batch_size
        result = BatchResult(total_records=len(records))
        max_timestamp_seen = None
        
        # Determine which fields to use based on actual DB columns
        db_columns = self._get_db_columns()
        
        # Collect all fields present in the records that also exist in the DB
        # Use the first record as a representative sample, but include ListingKey always
        sample_fields = set()
        for record in records[:10]:  # Sample first 10 records for field detection
            sample_fields.update(record.keys())
        
        # Filter to only fields that exist as DB columns, exclude internal fields
        fields = sorted(
            f for f in sample_fields
            if f in db_columns and not f.startswith('_')
        )
        
        # Ensure ListingKey is always first (it's the primary key)
        if 'ListingKey' in fields:
            fields.remove('ListingKey')
        fields = ['ListingKey'] + fields
        
        self.logger.info(f"Upserting with {len(fields)} fields (out of {len(sample_fields)} in records, {len(db_columns)} in DB)")
        
        try:
            with self.connection_context() as conn:
                with conn.cursor() as cursor:
                    # Build the upsert SQL dynamically from detected fields
                    fields_str = ', '.join(fields)
                    placeholders = ', '.join(['%s'] * len(fields))
                    update_fields = [f for f in fields if f != 'ListingKey']
                    update_clause = ', '.join([f"{f} = VALUES({f})" for f in update_fields])
                    
                    upsert_sql = f"""
                        INSERT INTO properties ({fields_str}) 
                        VALUES ({placeholders})
                        ON DUPLICATE KEY UPDATE {update_clause}
                    """
                    
                    checkpoint_num = 0
                    
                    # Process records in checkpoint-sized chunks
                    for cp_start in range(0, len(records), checkpoint_size):
                        cp_end = min(cp_start + checkpoint_size, len(records))
                        checkpoint_records = records[cp_start:cp_end]
                        
                        # Determine inserts vs updates for this checkpoint
                        listing_keys = [r.get('ListingKey') for r in checkpoint_records if r.get('ListingKey')]
                        existing_keys = set()
                        if listing_keys:
                            # Query in sub-batches to avoid overly large IN clauses
                            for i in range(0, len(listing_keys), 1000):
                                key_batch = listing_keys[i:i + 1000]
                                key_placeholders = ', '.join(['%s'] * len(key_batch))
                                cursor.execute(
                                    f"SELECT ListingKey FROM properties WHERE ListingKey IN ({key_placeholders})",
                                    key_batch
                                )
                                existing_keys.update(row['ListingKey'] for row in cursor.fetchall())
                        
                        cp_inserted = 0
                        cp_updated = 0
                        
                        for record in checkpoint_records:
                            key = record.get('ListingKey')
                            if key in existing_keys:
                                cp_updated += 1
                            else:
                                cp_inserted += 1
                        
                        # Execute upserts in SQL batch_size chunks within this checkpoint
                        for i in range(0, len(checkpoint_records), batch_size):
                            batch = checkpoint_records[i:i + batch_size]
                            batch_values = []
                            
                            for record in batch:
                                prepared = self._prepare_record_values(record, fields)
                                values = tuple(prepared.get(f) for f in fields)
                                batch_values.append(values)
                                
                                # Track max timestamp
                                ts = record.get(timestamp_field)
                                if ts:
                                    if isinstance(ts, str):
                                        try:
                                            ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                                        except (ValueError, TypeError):
                                            ts = None
                                    if ts and (max_timestamp_seen is None or ts > max_timestamp_seen):
                                        max_timestamp_seen = ts
                            
                            try:
                                cursor.executemany(upsert_sql, batch_values)
                            except pymysql.Error as e:
                                result.errors += len(batch)
                                result.error_messages.append(
                                    f"Checkpoint {checkpoint_num}, batch {i//batch_size}: {str(e)}"
                                )
                                self.logger.warning(f"Batch upsert error at checkpoint {checkpoint_num}: {str(e)}")
                                continue
                        
                        # COMMIT this checkpoint
                        conn.commit()
                        
                        result.inserted += cp_inserted
                        result.updated += cp_updated
                        checkpoint_num += 1
                        total_committed = cp_end
                        
                        self.logger.info(
                            f"Checkpoint {checkpoint_num}: committed {total_committed}/{len(records)} records "
                            f"(+{cp_inserted} inserted, +{cp_updated} updated)"
                        )
                        
                        # Update sync run watermark so recovery knows where we left off
                        if sync_run and sync_run.sync_id and max_timestamp_seen:
                            try:
                                cursor.execute(
                                    """
                                    UPDATE etl_sync_log SET
                                        records_processed = %s,
                                        records_inserted = %s,
                                        records_updated = %s,
                                        last_sync_timestamp = %s
                                    WHERE id = %s
                                    """,
                                    (
                                        total_committed,
                                        result.inserted,
                                        result.updated,
                                        max_timestamp_seen,
                                        sync_run.sync_id
                                    )
                                )
                                conn.commit()
                            except pymysql.Error as e:
                                self.logger.warning(
                                    f"Failed to update sync watermark at checkpoint {checkpoint_num}: {e}"
                                )
                        
                        # Invoke callback if provided
                        if on_checkpoint:
                            try:
                                on_checkpoint(checkpoint_num, total_committed, result)
                            except Exception:
                                pass  # Don't let callback errors break the sync
                    
        except pymysql.Error as e:
            raise DatabaseError(
                f"Checkpoint upsert failed at record ~{result.inserted + result.updated + result.errors}: {str(e)}"
            )
        
        return result

    # Sync metadata tracking methods
    
    def start_sync_run(self) -> SyncRun:
        """
        Start a new sync run and record it in the database.
        
        Returns:
            SyncRun object with sync_id populated.
            
        Raises:
            DatabaseError: If sync run creation fails.
        """
        sync_run = SyncRun(
            sync_start=datetime.now(),
            status=SyncStatus.SUCCESS
        )
        
        try:
            with self.connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO etl_sync_log (sync_start, status)
                        VALUES (%s, %s)
                        """,
                        (sync_run.sync_start, sync_run.status.value)
                    )
                    sync_run.sync_id = cursor.lastrowid
                    conn.commit()
                    
            self._current_sync_run = sync_run
            self.logger.info(f"Started sync run {sync_run.sync_id}")
            return sync_run
            
        except pymysql.Error as e:
            raise DatabaseError(f"Failed to start sync run: {str(e)}")
    
    def update_sync_run(
        self,
        sync_run: SyncRun,
        records_processed: Optional[int] = None,
        records_inserted: Optional[int] = None,
        records_updated: Optional[int] = None,
        api_calls_made: Optional[int] = None,
        status: Optional[SyncStatus] = None,
        error_message: Optional[str] = None
    ) -> SyncRun:
        """
        Update sync run metadata.
        
        Args:
            sync_run: SyncRun to update.
            records_processed: Total records processed.
            records_inserted: Records inserted.
            records_updated: Records updated.
            api_calls_made: API calls made.
            status: Sync status.
            error_message: Error message if any.
            
        Returns:
            Updated SyncRun object.
            
        Raises:
            DatabaseError: If update fails.
        """
        if sync_run.sync_id is None:
            raise DatabaseError("Cannot update sync run without sync_id")
        
        # Update local object
        if records_processed is not None:
            sync_run.records_processed = records_processed
        if records_inserted is not None:
            sync_run.records_inserted = records_inserted
        if records_updated is not None:
            sync_run.records_updated = records_updated
        if api_calls_made is not None:
            sync_run.api_calls_made = api_calls_made
        if status is not None:
            sync_run.status = status
        if error_message is not None:
            sync_run.error_message = error_message
        
        try:
            with self.connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE etl_sync_log SET
                            records_processed = %s,
                            records_inserted = %s,
                            records_updated = %s,
                            api_calls_made = %s,
                            status = %s,
                            error_message = %s
                        WHERE id = %s
                        """,
                        (
                            sync_run.records_processed,
                            sync_run.records_inserted,
                            sync_run.records_updated,
                            sync_run.api_calls_made,
                            sync_run.status.value,
                            sync_run.error_message,
                            sync_run.sync_id
                        )
                    )
                    conn.commit()
                    
            return sync_run
            
        except pymysql.Error as e:
            raise DatabaseError(f"Failed to update sync run: {str(e)}")
    
    def complete_sync_run(
        self,
        sync_run: SyncRun,
        status: SyncStatus = SyncStatus.SUCCESS,
        error_message: Optional[str] = None
    ) -> SyncRun:
        """
        Complete a sync run and record final metadata.
        
        Args:
            sync_run: SyncRun to complete.
            status: Final status.
            error_message: Error message if any.
            
        Returns:
            Completed SyncRun object.
            
        Raises:
            DatabaseError: If completion fails.
        """
        if sync_run.sync_id is None:
            raise DatabaseError("Cannot complete sync run without sync_id")
        
        sync_run.sync_end = datetime.now()
        sync_run.status = status
        if error_message:
            sync_run.error_message = error_message
        
        try:
            with self.connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE etl_sync_log SET
                            sync_end = %s,
                            records_processed = %s,
                            records_inserted = %s,
                            records_updated = %s,
                            api_calls_made = %s,
                            status = %s,
                            error_message = %s,
                            last_sync_timestamp = %s
                        WHERE id = %s
                        """,
                        (
                            sync_run.sync_end,
                            sync_run.records_processed,
                            sync_run.records_inserted,
                            sync_run.records_updated,
                            sync_run.api_calls_made,
                            sync_run.status.value,
                            sync_run.error_message,
                            sync_run.last_sync_timestamp,
                            sync_run.sync_id
                        )
                    )
                    conn.commit()
            
            self._current_sync_run = None
            self.logger.info(
                f"Completed sync run {sync_run.sync_id}: "
                f"status={status.value}, processed={sync_run.records_processed}, "
                f"inserted={sync_run.records_inserted}, updated={sync_run.records_updated}"
            )
            return sync_run
            
        except pymysql.Error as e:
            raise DatabaseError(f"Failed to complete sync run: {str(e)}")
    
    def get_last_successful_sync(self) -> Optional[SyncRun]:
        """
        Get the last successful sync run.
        
        Returns:
            Last successful SyncRun or None if no successful syncs.
            
        Raises:
            DatabaseError: If query fails.
        """
        try:
            with self.connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT id, sync_start, sync_end, records_processed,
                               records_inserted, records_updated, api_calls_made,
                               status, error_message, last_sync_timestamp
                        FROM etl_sync_log
                        WHERE status = 'success'
                        ORDER BY sync_end DESC
                        LIMIT 1
                        """
                    )
                    row = cursor.fetchone()
                    
                    if row:
                        return SyncRun(
                            sync_id=row['id'],
                            sync_start=row['sync_start'],
                            sync_end=row['sync_end'],
                            records_processed=row['records_processed'],
                            records_inserted=row['records_inserted'],
                            records_updated=row['records_updated'],
                            api_calls_made=row['api_calls_made'],
                            status=SyncStatus(row['status']),
                            error_message=row['error_message'],
                            last_sync_timestamp=row['last_sync_timestamp']
                        )
                    return None
                    
        except pymysql.Error as e:
            raise DatabaseError(f"Failed to get last successful sync: {str(e)}")
    
    def get_last_sync_timestamp(self) -> Optional[datetime]:
        """
        Get the timestamp of the last successful sync.
        
        Returns:
            Last sync timestamp or None if no successful syncs.
            
        Raises:
            DatabaseError: If query fails.
        """
        last_sync = self.get_last_successful_sync()
        if last_sync:
            return last_sync.last_sync_timestamp or last_sync.sync_end
        return None
    
    def get_sync_history(self, limit: int = 10) -> List[SyncRun]:
        """
        Get recent sync run history.
        
        Args:
            limit: Maximum number of records to return.
            
        Returns:
            List of recent SyncRun objects.
            
        Raises:
            DatabaseError: If query fails.
        """
        try:
            with self.connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT id, sync_start, sync_end, records_processed,
                               records_inserted, records_updated, api_calls_made,
                               status, error_message, last_sync_timestamp
                        FROM etl_sync_log
                        ORDER BY sync_start DESC
                        LIMIT %s
                        """,
                        (limit,)
                    )
                    rows = cursor.fetchall()
                    
                    return [
                        SyncRun(
                            sync_id=row['id'],
                            sync_start=row['sync_start'],
                            sync_end=row['sync_end'],
                            records_processed=row['records_processed'],
                            records_inserted=row['records_inserted'],
                            records_updated=row['records_updated'],
                            api_calls_made=row['api_calls_made'],
                            status=SyncStatus(row['status']),
                            error_message=row['error_message'],
                            last_sync_timestamp=row['last_sync_timestamp']
                        )
                        for row in rows
                    ]
                    
        except pymysql.Error as e:
            raise DatabaseError(f"Failed to get sync history: {str(e)}")
    
    def get_record_count(self) -> int:
        """
        Get total number of records in properties table.
        
        Returns:
            Total record count.
            
        Raises:
            DatabaseError: If query fails.
        """
        try:
            with self.connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) as count FROM properties")
                    row = cursor.fetchone()
                    return row['count'] if row else 0
                    
        except pymysql.Error as e:
            raise DatabaseError(f"Failed to get record count: {str(e)}")
    
    def get_record_by_key(self, listing_key: str) -> Optional[Dict[str, Any]]:
        """
        Get a single record by listing key.
        
        Args:
            listing_key: The listing key to look up.
            
        Returns:
            Record dictionary or None if not found.
            
        Raises:
            DatabaseError: If query fails.
        """
        try:
            with self.connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT * FROM properties WHERE ListingKey = %s",
                        (listing_key,)
                    )
                    return cursor.fetchone()
                    
        except pymysql.Error as e:
            raise DatabaseError(f"Failed to get record: {str(e)}")
    
    def delete_record(self, listing_key: str) -> bool:
        """
        Delete a record by listing key.
        
        Args:
            listing_key: The listing key to delete.
            
        Returns:
            True if record was deleted, False if not found.
            
        Raises:
            DatabaseError: If deletion fails.
        """
        try:
            with self.connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM properties WHERE ListingKey = %s",
                        (listing_key,)
                    )
                    deleted = cursor.rowcount > 0
                    conn.commit()
                    return deleted
                    
        except pymysql.Error as e:
            raise DatabaseError(f"Failed to delete record: {str(e)}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_connection()
