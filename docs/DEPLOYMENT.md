# Deployment Guide

Complete setup and deployment instructions for the Trestle ETL Pipeline.

## System Requirements

### Hardware
- **CPU**: 1+ cores (2+ recommended for production)
- **RAM**: 512MB minimum, 1GB+ recommended
- **Disk**: 1GB for application + space for logs

### Software
- Python 3.8 or higher
- MySQL 5.7+ or MariaDB 10.3+
- pip (Python package manager)

## Installation Steps

### 1. Clone Repository

```bash
git clone <repository-url>
cd trestle-etl
```

### 2. Create Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate (Linux/macOS)
source venv/bin/activate

# Activate (Windows)
venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment

```bash
# Copy template
cp .env.example .env

# Edit with your settings
nano .env  # or your preferred editor
```

#### Required Configuration

| Variable | Description | Example |
|----------|-------------|---------|
| `TRESTLE_CLIENT_ID` | API client ID | `your_client_id` |
| `TRESTLE_CLIENT_SECRET` | API client secret | `your_secret` |
| `MYSQL_HOST` | Database host | `localhost` |
| `MYSQL_DATABASE` | Database name | `real_estate` |
| `MYSQL_USER` | Database user | `etl_user` |
| `MYSQL_PASSWORD` | Database password | `secure_password` |

#### Optional Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MYSQL_PORT` | `3306` | Database port |
| `BATCH_SIZE` | `1000` | Records per batch |
| `MAX_RETRIES` | `3` | Retry attempts |
| `LOG_LEVEL` | `INFO` | Logging verbosity |
| `LOG_DIR` | `logs` | Log directory |

### 5. Setup Database

Create the database and user:

```sql
-- Create database
CREATE DATABASE real_estate CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create user
CREATE USER 'etl_user'@'localhost' IDENTIFIED BY 'secure_password';

-- Grant permissions
GRANT ALL PRIVILEGES ON real_estate.* TO 'etl_user'@'localhost';
FLUSH PRIVILEGES;
```

The ETL will automatically create required tables on first run.

### 6. Verify Installation

```bash
# Test configuration
python -m trestle_etl.main --dry-run --verbose

# Run initial sync
python -m trestle_etl.main --full-sync
```

## Production Deployment

### Directory Structure

```bash
/opt/trestle-etl/
├── venv/
├── trestle_etl/
├── logs/
├── .env
└── requirements.txt
```

### File Permissions

```bash
# Set ownership
sudo chown -R etl_user:etl_user /opt/trestle-etl

# Protect credentials
chmod 600 /opt/trestle-etl/.env

# Make logs writable
chmod 755 /opt/trestle-etl/logs
```

### Systemd Service (Optional)

Create `/etc/systemd/system/trestle-etl.service`:

```ini
[Unit]
Description=Trestle ETL Pipeline
After=network.target mysql.service

[Service]
Type=oneshot
User=etl_user
WorkingDirectory=/opt/trestle-etl
Environment=PATH=/opt/trestle-etl/venv/bin
ExecStart=/opt/trestle-etl/venv/bin/python -m trestle_etl.main
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable trestle-etl
```

### Log Rotation

Create `/etc/logrotate.d/trestle-etl`:

```
/opt/trestle-etl/logs/*.log {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
    create 644 etl_user etl_user
}
```

## Environment-Specific Configurations

### Development

```bash
# .env.development
LOG_LEVEL=DEBUG
BATCH_SIZE=100
MAX_RETRIES=1
```

### Staging

```bash
# .env.staging
LOG_LEVEL=INFO
BATCH_SIZE=500
MAX_RETRIES=2
```

### Production

```bash
# .env.production
LOG_LEVEL=WARNING
BATCH_SIZE=1000
MAX_RETRIES=3
ALERT_EMAIL_ENABLED=true
ALERT_EMAIL_RECIPIENTS=ops@company.com
```

## Upgrading

```bash
# Stop any running ETL
# Pull latest code
git pull origin main

# Update dependencies
pip install -r requirements.txt --upgrade

# Test
python -m trestle_etl.main --dry-run

# Resume normal operation
```

## Monitoring

### Log Files

- `logs/etl_main.log` - Main execution log
- `logs/api_calls.log` - API request details
- `logs/errors.log` - Error details

### Health Checks

```bash
# Check last sync status
mysql -u etl_user -p real_estate -e "SELECT * FROM etl_sync_log ORDER BY sync_start DESC LIMIT 5;"

# Check record counts
mysql -u etl_user -p real_estate -e "SELECT COUNT(*) FROM properties;"
```

## Security Considerations

1. **Credentials**: Store in `.env` file with restricted permissions (600)
2. **Database**: Use dedicated user with minimal required permissions
3. **Network**: Use SSL/TLS for database connections in production
4. **Logs**: Ensure logs don't contain sensitive data
