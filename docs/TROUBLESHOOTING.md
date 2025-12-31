# Troubleshooting Guide

Common issues and solutions for the Trestle ETL Pipeline.

## Quick Diagnostics

```bash
# Check last sync status
python -c "
from trestle_etl.config import ConfigManager
from trestle_etl.mysql_loader import MySQLLoader
config = ConfigManager().load_config()
loader = MySQLLoader(config.database)
history = loader.get_sync_history(5)
for sync in history:
    print(f'{sync.sync_start}: {sync.status.value} - {sync.records_processed} records')
"
```

## Common Issues

### 1. Configuration Errors

#### Missing Environment Variables

**Error:**
```
ConfigurationError: Missing required environment variables: TRESTLE_CLIENT_ID, TRESTLE_CLIENT_SECRET
```

**Solution:**
1. Ensure `.env` file exists in the working directory
2. Verify all required variables are set:
```bash
cat .env | grep -E "^(TRESTLE_|MYSQL_)"
```

#### Invalid .env File Path

**Error:**
```
ConfigurationError: Environment file not found: /path/to/.env
```

**Solution:**
```bash
# Check file exists
ls -la .env

# Use explicit path
python -m trestle_etl.main --config /full/path/to/.env
```

### 2. Authentication Failures

#### Invalid Credentials

**Error:**
```
ODataError: Authentication failed: invalid_client
```

**Solution:**
1. Verify credentials in `.env`:
```bash
echo $TRESTLE_CLIENT_ID
echo $TRESTLE_CLIENT_SECRET
```
2. Check credentials with CoreLogic
3. Ensure no extra whitespace in values

#### Token Expiration

**Error:**
```
ODataError: 401 Unauthorized
```

**Solution:**
- Tokens are automatically refreshed (8-hour validity)
- If persistent, check system clock synchronization:
```bash
timedatectl status
```

### 3. Database Connection Issues

#### Connection Refused

**Error:**
```
ConnectionError: Failed to connect to database: Can't connect to MySQL server
```

**Solution:**
1. Verify MySQL is running:
```bash
systemctl status mysql
```
2. Check connection parameters:
```bash
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u $MYSQL_USER -p
```
3. Verify firewall allows connection

#### Access Denied

**Error:**
```
ConnectionError: Access denied for user 'etl_user'@'localhost'
```

**Solution:**
```sql
-- Check user exists
SELECT user, host FROM mysql.user WHERE user = 'etl_user';

-- Grant permissions
GRANT ALL PRIVILEGES ON real_estate.* TO 'etl_user'@'localhost';
FLUSH PRIVILEGES;
```

#### Database Does Not Exist

**Error:**
```
DatabaseError: Unknown database 'real_estate'
```

**Solution:**
```sql
CREATE DATABASE real_estate CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

### 4. Rate Limiting

#### API Quota Exceeded

**Error:**
```
RateLimitError: Rate limit exceeded. Retry after 60 seconds.
```

**Solution:**
1. Wait for quota reset (automatic with exponential backoff)
2. Reduce sync frequency in cron schedule
3. Reduce batch size:
```bash
# In .env
BATCH_SIZE=500
```

#### Persistent Rate Limits

If rate limits persist:
1. Check API usage in logs:
```bash
grep "quota" logs/api_calls.log
```
2. Contact CoreLogic about quota limits
3. Implement longer intervals between syncs

### 5. Execution Lock Issues

#### ETL Already Running

**Error:**
```
ETL is already running (PID: 12345, started: 2024-01-15T10:30:00)
```

**Solution:**
1. Check if previous process is still running:
```bash
ps aux | grep trestle_etl
```
2. If process is stuck, kill it:
```bash
kill 12345
```
3. Remove stale lock file:
```bash
rm /tmp/trestle_etl.lock
```

#### Lock File Permission Issues

**Error:**
```
LockAcquisitionError: Permission denied
```

**Solution:**
```bash
# Check lock file location
ls -la /tmp/trestle_etl.lock

# Change lock file location in .env
LOCK_FILE_PATH=/opt/trestle-etl/etl.lock
```

### 6. Data Transformation Errors

#### Invalid Data Types

**Error:**
```
DataTransformationError: Cannot convert value 'N/A' to integer
```

**Solution:**
- These are logged and skipped automatically
- Check `logs/data_quality.log` for details
- Invalid records don't stop processing

#### Missing Required Fields

**Error:**
```
ValidationError: Missing required field: ListingKey
```

**Solution:**
- Check API response format
- Verify OData query includes required fields
- Review `logs/data_quality.log`

### 7. Memory Issues

#### Out of Memory

**Error:**
```
MemoryError: Unable to allocate array
```

**Solution:**
1. Reduce batch size:
```bash
BATCH_SIZE=250
```
2. Enable resource monitoring (automatic)
3. Add swap space if needed:
```bash
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

### 8. Network Issues

#### Connection Timeout

**Error:**
```
requests.exceptions.Timeout: Connection timed out
```

**Solution:**
1. Increase timeout:
```bash
TRESTLE_API_TIMEOUT=60
```
2. Check network connectivity:
```bash
curl -I https://api-prod.corelogic.com
```
3. Verify DNS resolution:
```bash
nslookup api-prod.corelogic.com
```

#### SSL Certificate Errors

**Error:**
```
SSLError: certificate verify failed
```

**Solution:**
1. Update CA certificates:
```bash
sudo apt-get update && sudo apt-get install ca-certificates
```
2. Check system time is correct

## Log Analysis

### Finding Errors

```bash
# Recent errors
tail -100 logs/errors.log

# Search for specific error
grep -i "authentication" logs/*.log

# Count errors by type
grep -h "Error" logs/errors.log | sort | uniq -c | sort -rn
```

### Performance Analysis

```bash
# Check sync durations
grep "duration" logs/etl_main.log | tail -20

# API call statistics
grep "api_calls" logs/etl_main.log | tail -10
```

## Recovery Procedures

### After Failed Sync

1. Check error logs:
```bash
tail -50 logs/errors.log
```

2. Fix the issue (credentials, network, etc.)

3. Run incremental sync:
```bash
python -m trestle_etl.main --verbose
```

### Data Inconsistency

If data appears inconsistent:

1. Check sync history:
```sql
SELECT * FROM etl_sync_log ORDER BY sync_start DESC LIMIT 10;
```

2. Run full sync to reset:
```bash
python -m trestle_etl.main --full-sync
```

### Complete Reset

To start fresh:

```sql
-- Backup first!
-- Then truncate tables
TRUNCATE TABLE properties;
TRUNCATE TABLE etl_sync_log;
```

```bash
# Run full sync
python -m trestle_etl.main --full-sync
```

## Getting Help

1. Check logs in `logs/` directory
2. Review this troubleshooting guide
3. Check CoreLogic Trestle API documentation
4. Contact support with:
   - Error messages
   - Relevant log excerpts
   - Configuration (without credentials)
