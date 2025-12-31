#!/usr/bin/env python
"""
Database Schema Migration Script for Trestle ETL Pipeline.

This script manages database schema migrations, including:
- Initial schema creation
- Schema version tracking
- Incremental migrations

Usage:
    python scripts/migrate_schema.py [--config PATH] [--dry-run] [--version VERSION]
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pymysql
from pymysql.cursors import DictCursor

from trestle_etl.config import ConfigManager, ConfigurationError


# Schema version tracking table
SCHEMA_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INT PRIMARY KEY,
    description VARCHAR(255) NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_applied_at (applied_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

# Migration definitions: (version, description, up_sql, down_sql)
MIGRATIONS: List[Tuple[int, str, str, str]] = [
    (
        1,
        "Create properties table",
        """
        CREATE TABLE IF NOT EXISTS properties (
            listing_key VARCHAR(255) PRIMARY KEY,
            list_price DECIMAL(12,2),
            property_type VARCHAR(100),
            bedrooms_total INT,
            bathrooms_total DECIMAL(3,1),
            square_feet INT,
            lot_size_acres DECIMAL(10,4),
            year_built INT,
            listing_status VARCHAR(50),
            modification_timestamp DATETIME,
            street_address VARCHAR(255),
            city VARCHAR(100),
            state_or_province VARCHAR(50),
            postal_code VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_modification_timestamp (modification_timestamp),
            INDEX idx_property_type (property_type),
            INDEX idx_listing_status (listing_status),
            INDEX idx_city (city),
            INDEX idx_postal_code (postal_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        "DROP TABLE IF EXISTS properties"
    ),
    (
        2,
        "Create ETL sync log table",
        """
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
        """,
        "DROP TABLE IF EXISTS etl_sync_log"
    ),
    (
        3,
        "Add latitude and longitude to properties",
        """
        ALTER TABLE properties
        ADD COLUMN latitude DECIMAL(10, 8) NULL AFTER postal_code,
        ADD COLUMN longitude DECIMAL(11, 8) NULL AFTER latitude,
        ADD INDEX idx_location (latitude, longitude)
        """,
        """
        ALTER TABLE properties
        DROP INDEX idx_location,
        DROP COLUMN longitude,
        DROP COLUMN latitude
        """
    ),
    (
        4,
        "Add county and MLS fields to properties",
        """
        ALTER TABLE properties
        ADD COLUMN county VARCHAR(100) NULL AFTER state_or_province,
        ADD COLUMN mls_id VARCHAR(50) NULL AFTER listing_key,
        ADD COLUMN mls_name VARCHAR(100) NULL AFTER mls_id,
        ADD INDEX idx_county (county),
        ADD INDEX idx_mls_id (mls_id)
        """,
        """
        ALTER TABLE properties
        DROP INDEX idx_mls_id,
        DROP INDEX idx_county,
        DROP COLUMN mls_name,
        DROP COLUMN mls_id,
        DROP COLUMN county
        """
    ),
]


class SchemaMigrator:
    """Manages database schema migrations."""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        """Initialize migrator with database connection parameters."""
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self._connection: Optional[pymysql.Connection] = None
    
    def connect(self) -> pymysql.Connection:
        """Create database connection."""
        if self._connection is None or not self._connection.open:
            self._connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
                cursorclass=DictCursor,
                autocommit=False
            )
        return self._connection
    
    def close(self) -> None:
        """Close database connection."""
        if self._connection and self._connection.open:
            self._connection.close()
            self._connection = None
    
    def ensure_migration_table(self) -> None:
        """Create schema_migrations table if it doesn't exist."""
        conn = self.connect()
        with conn.cursor() as cursor:
            cursor.execute(SCHEMA_VERSION_TABLE)
        conn.commit()
    
    def get_current_version(self) -> int:
        """Get the current schema version."""
        self.ensure_migration_table()
        conn = self.connect()
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(version) as version FROM schema_migrations")
            result = cursor.fetchone()
            return result['version'] if result and result['version'] else 0
    
    def get_applied_migrations(self) -> List[int]:
        """Get list of applied migration versions."""
        self.ensure_migration_table()
        conn = self.connect()
        with conn.cursor() as cursor:
            cursor.execute("SELECT version FROM schema_migrations ORDER BY version")
            return [row['version'] for row in cursor.fetchall()]
    
    def migrate_up(self, target_version: Optional[int] = None, dry_run: bool = False) -> List[str]:
        """
        Apply pending migrations up to target version.
        
        Args:
            target_version: Target version (None = latest)
            dry_run: If True, don't actually apply migrations
            
        Returns:
            List of applied migration descriptions
        """
        applied = self.get_applied_migrations()
        pending = [m for m in MIGRATIONS if m[0] not in applied]
        
        if target_version is not None:
            pending = [m for m in pending if m[0] <= target_version]
        
        pending.sort(key=lambda m: m[0])
        
        results = []
        conn = self.connect()
        
        for version, description, up_sql, _ in pending:
            print(f"{'[DRY RUN] ' if dry_run else ''}Applying migration {version}: {description}")
            
            if not dry_run:
                try:
                    with conn.cursor() as cursor:
                        # Apply migration
                        cursor.execute(up_sql)
                        
                        # Record migration
                        cursor.execute(
                            "INSERT INTO schema_migrations (version, description) VALUES (%s, %s)",
                            (version, description)
                        )
                    conn.commit()
                    results.append(f"Applied: {version} - {description}")
                except pymysql.Error as e:
                    conn.rollback()
                    raise RuntimeError(f"Migration {version} failed: {e}")
            else:
                results.append(f"Would apply: {version} - {description}")
        
        return results
    
    def migrate_down(self, target_version: int, dry_run: bool = False) -> List[str]:
        """
        Rollback migrations down to target version.
        
        Args:
            target_version: Target version to rollback to
            dry_run: If True, don't actually rollback
            
        Returns:
            List of rolled back migration descriptions
        """
        applied = self.get_applied_migrations()
        to_rollback = [m for m in MIGRATIONS if m[0] in applied and m[0] > target_version]
        to_rollback.sort(key=lambda m: m[0], reverse=True)
        
        results = []
        conn = self.connect()
        
        for version, description, _, down_sql in to_rollback:
            print(f"{'[DRY RUN] ' if dry_run else ''}Rolling back migration {version}: {description}")
            
            if not dry_run:
                try:
                    with conn.cursor() as cursor:
                        # Apply rollback
                        cursor.execute(down_sql)
                        
                        # Remove migration record
                        cursor.execute(
                            "DELETE FROM schema_migrations WHERE version = %s",
                            (version,)
                        )
                    conn.commit()
                    results.append(f"Rolled back: {version} - {description}")
                except pymysql.Error as e:
                    conn.rollback()
                    raise RuntimeError(f"Rollback of migration {version} failed: {e}")
            else:
                results.append(f"Would rollback: {version} - {description}")
        
        return results
    
    def status(self) -> dict:
        """Get migration status."""
        current = self.get_current_version()
        applied = self.get_applied_migrations()
        latest = max(m[0] for m in MIGRATIONS) if MIGRATIONS else 0
        pending = [m for m in MIGRATIONS if m[0] not in applied]
        
        return {
            'current_version': current,
            'latest_version': latest,
            'applied_count': len(applied),
            'pending_count': len(pending),
            'pending_migrations': [(m[0], m[1]) for m in pending],
            'is_up_to_date': current == latest
        }


def main():
    """Main entry point for migration script."""
    parser = argparse.ArgumentParser(
        description='Database schema migration tool for Trestle ETL Pipeline'
    )
    parser.add_argument(
        '--config', '-c',
        type=str,
        default=None,
        help='Path to .env configuration file'
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Show what would be done without making changes'
    )
    parser.add_argument(
        '--version', '-v',
        type=int,
        default=None,
        help='Target schema version (default: latest)'
    )
    parser.add_argument(
        '--rollback', '-r',
        action='store_true',
        help='Rollback to specified version instead of migrating up'
    )
    parser.add_argument(
        '--status', '-s',
        action='store_true',
        help='Show migration status'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    try:
        config_manager = ConfigManager(env_file=args.config)
        config = config_manager.load_config()
    except ConfigurationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 1
    
    # Create migrator
    db = config.database
    migrator = SchemaMigrator(
        host=db.host,
        port=db.port,
        database=db.database,
        user=db.user,
        password=db.password
    )
    
    try:
        if args.status:
            # Show status
            status = migrator.status()
            print(f"\nSchema Migration Status")
            print(f"=======================")
            print(f"Current version: {status['current_version']}")
            print(f"Latest version:  {status['latest_version']}")
            print(f"Applied:         {status['applied_count']}")
            print(f"Pending:         {status['pending_count']}")
            
            if status['pending_migrations']:
                print(f"\nPending migrations:")
                for v, desc in status['pending_migrations']:
                    print(f"  {v}: {desc}")
            else:
                print(f"\nSchema is up to date!")
            
            return 0
        
        if args.rollback:
            if args.version is None:
                print("Error: --version is required for rollback", file=sys.stderr)
                return 1
            
            results = migrator.migrate_down(args.version, dry_run=args.dry_run)
        else:
            results = migrator.migrate_up(args.version, dry_run=args.dry_run)
        
        if results:
            print(f"\n{'Dry run ' if args.dry_run else ''}Results:")
            for result in results:
                print(f"  {result}")
        else:
            print("\nNo migrations to apply.")
        
        return 0
        
    except Exception as e:
        print(f"Migration error: {e}", file=sys.stderr)
        return 1
    finally:
        migrator.close()


if __name__ == '__main__':
    sys.exit(main())
