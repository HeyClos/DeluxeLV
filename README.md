# Trestle ETL Pipeline

A cost-effective Python ETL pipeline that extracts real estate data from CoreLogic's Trestle API and loads it into a MySQL database.

## Features

- **OData Protocol Support**: Efficient data extraction using OData filtering and pagination
- **Incremental Updates**: Only fetches changed records since last sync
- **Cost Optimization**: Minimizes API calls through batching and smart filtering
- **Robust Error Handling**: Exponential backoff, retry logic, and graceful degradation
- **Comprehensive Logging**: Rotating log files with detailed execution tracking
- **Alerting**: Optional email and webhook notifications for critical errors
- **Execution Locking**: Prevents concurrent ETL runs

## Quick Start

### Prerequisites

- Python 3.8+
- MySQL 5.7+ or MariaDB 10.3+
- CoreLogic Trestle API credentials

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd trestle-etl

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Configuration

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit `.env` with your credentials:
```bash
# Required settings
TRESTLE_CLIENT_ID=your_client_id
TRESTLE_CLIENT_SECRET=your_client_secret
MYSQL_HOST=localhost
MYSQL_DATABASE=real_estate
MYSQL_USER=etl_user
MYSQL_PASSWORD=your_password
```

### Running the ETL

```bash
# Run incremental sync (default)
python -m trestle_etl.main

# Run full sync
python -m trestle_etl.main --full-sync

# Dry run (no database writes)
python -m trestle_etl.main --dry-run

# Verbose output
python -m trestle_etl.main --verbose
```

## Documentation

- [Deployment Guide](docs/DEPLOYMENT.md) - Complete setup and deployment instructions
- [Troubleshooting Guide](docs/TROUBLESHOOTING.md) - Common issues and solutions
- [Cron Scheduling](docs/CRON_EXAMPLES.md) - Automated scheduling examples

## Project Structure

```
trestle-etl/
├── trestle_etl/           # Main package
│   ├── __init__.py
│   ├── main.py            # ETL orchestration
│   ├── config.py          # Configuration management
│   ├── odata_client.py    # Trestle API client
│   ├── data_transformer.py # Data transformation
│   ├── mysql_loader.py    # Database operations
│   ├── incremental_sync.py # Incremental update logic
│   ├── cost_monitor.py    # API usage tracking
│   ├── logger.py          # Logging infrastructure
│   └── alerting.py        # Alert notifications
├── tests/                 # Test suite
├── docs/                  # Documentation
├── logs/                  # Log files (created at runtime)
├── .env.example           # Configuration template
├── requirements.txt       # Python dependencies
└── README.md
```

## License

MIT License - see LICENSE file for details.
