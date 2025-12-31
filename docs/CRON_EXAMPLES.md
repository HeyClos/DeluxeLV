# Cron Scheduling Examples

Configure automated ETL execution using cron jobs.

## Basic Cron Syntax

```
* * * * * command
│ │ │ │ │
│ │ │ │ └── Day of week (0-7, Sunday=0 or 7)
│ │ │ └──── Month (1-12)
│ │ └────── Day of month (1-31)
│ └──────── Hour (0-23)
└────────── Minute (0-59)
```

## Recommended Schedules

### Every 6 Hours (Cost-Effective)

```bash
# Run at midnight, 6am, noon, and 6pm
0 0,6,12,18 * * * /opt/trestle-etl/venv/bin/python -m trestle_etl.main >> /opt/trestle-etl/logs/cron.log 2>&1
```

### Every 4 Hours

```bash
# Run every 4 hours
0 */4 * * * /opt/trestle-etl/venv/bin/python -m trestle_etl.main >> /opt/trestle-etl/logs/cron.log 2>&1
```

### Hourly

```bash
# Run at the start of every hour
0 * * * * /opt/trestle-etl/venv/bin/python -m trestle_etl.main >> /opt/trestle-etl/logs/cron.log 2>&1
```

### Twice Daily

```bash
# Run at 2am and 2pm
0 2,14 * * * /opt/trestle-etl/venv/bin/python -m trestle_etl.main >> /opt/trestle-etl/logs/cron.log 2>&1
```

### Daily (Low Cost)

```bash
# Run at 3am daily
0 3 * * * /opt/trestle-etl/venv/bin/python -m trestle_etl.main >> /opt/trestle-etl/logs/cron.log 2>&1
```

### Weekly Full Sync

```bash
# Full sync every Sunday at 1am
0 1 * * 0 /opt/trestle-etl/venv/bin/python -m trestle_etl.main --full-sync >> /opt/trestle-etl/logs/cron.log 2>&1
```

## Setting Up Cron Jobs

### Edit Crontab

```bash
# Edit current user's crontab
crontab -e

# Edit specific user's crontab (as root)
sudo crontab -u etl_user -e
```

### Example Complete Crontab

```bash
# Trestle ETL Pipeline Cron Jobs
# Environment setup
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin
MAILTO=admin@company.com

# Change to working directory and activate venv
ETL_DIR=/opt/trestle-etl
ETL_CMD=cd $ETL_DIR && source venv/bin/activate && python -m trestle_etl.main

# Incremental sync every 4 hours
0 */4 * * * $ETL_CMD >> $ETL_DIR/logs/cron.log 2>&1

# Weekly full sync on Sunday at 2am
0 2 * * 0 $ETL_CMD --full-sync >> $ETL_DIR/logs/cron.log 2>&1
```

## Using a Wrapper Script

Create `/opt/trestle-etl/run_etl.sh`:

```bash
#!/bin/bash
set -e

# Configuration
ETL_DIR="/opt/trestle-etl"
LOG_FILE="$ETL_DIR/logs/cron.log"
VENV_PATH="$ETL_DIR/venv/bin/activate"

# Change to ETL directory
cd "$ETL_DIR"

# Activate virtual environment
source "$VENV_PATH"

# Log start time
echo "========================================" >> "$LOG_FILE"
echo "ETL Started: $(date)" >> "$LOG_FILE"

# Run ETL with all arguments passed to script
python -m trestle_etl.main "$@" >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

# Log completion
echo "ETL Completed: $(date) (Exit code: $EXIT_CODE)" >> "$LOG_FILE"

exit $EXIT_CODE
```

Make executable:

```bash
chmod +x /opt/trestle-etl/run_etl.sh
```

Use in crontab:

```bash
0 */4 * * * /opt/trestle-etl/run_etl.sh
0 2 * * 0 /opt/trestle-etl/run_etl.sh --full-sync
```

## Monitoring Cron Jobs

### Check Cron Logs

```bash
# View recent cron executions
tail -f /opt/trestle-etl/logs/cron.log

# Check system cron log
grep trestle /var/log/syslog
```

### Verify Cron is Running

```bash
# List active cron jobs
crontab -l

# Check cron service status
systemctl status cron
```

## Cost Optimization Tips

| Schedule | API Calls/Day | Use Case |
|----------|---------------|----------|
| Daily | ~1-2 | Low-traffic sites |
| Every 6 hours | ~4-8 | Standard usage |
| Every 4 hours | ~6-12 | Active markets |
| Hourly | ~24-48 | High-frequency needs |

### Recommendations

1. **Start conservative**: Begin with daily syncs, increase frequency as needed
2. **Use incremental**: Always use incremental sync (default) except for weekly full syncs
3. **Off-peak hours**: Schedule during off-peak hours (2am-6am) when possible
4. **Monitor costs**: Check API usage in logs and adjust schedule accordingly

## Troubleshooting

### Cron Not Running

```bash
# Check if cron service is running
systemctl status cron

# Verify crontab syntax
crontab -l

# Check for errors in system log
grep CRON /var/log/syslog
```

### Permission Issues

```bash
# Ensure script is executable
chmod +x /opt/trestle-etl/run_etl.sh

# Check file ownership
ls -la /opt/trestle-etl/

# Verify user can access files
sudo -u etl_user cat /opt/trestle-etl/.env
```

### Environment Variables Not Loading

Cron runs with minimal environment. Ensure:

1. Use absolute paths in crontab
2. Source the virtual environment explicitly
3. Set PATH in crontab if needed
