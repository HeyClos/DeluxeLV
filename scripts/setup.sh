#!/bin/bash
# Trestle ETL Pipeline - Setup Script
# This script sets up the development environment

set -e

echo "=========================================="
echo "Trestle ETL Pipeline - Setup"
echo "=========================================="

# Check Python version
PYTHON_VERSION=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1,2)
REQUIRED_VERSION="3.8"

if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then
    echo "Error: Python $REQUIRED_VERSION or higher is required (found $PYTHON_VERSION)"
    exit 1
fi

echo "✓ Python version: $PYTHON_VERSION"

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment exists"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip > /dev/null

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt > /dev/null
echo "✓ Dependencies installed"

# Create logs directory
if [ ! -d "logs" ]; then
    mkdir -p logs
    echo "✓ Logs directory created"
else
    echo "✓ Logs directory exists"
fi

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        cp .env.example .env
        echo "✓ Created .env from template"
        echo ""
        echo "⚠️  IMPORTANT: Edit .env with your credentials before running the ETL"
    else
        echo "⚠️  Warning: .env.example not found, please create .env manually"
    fi
else
    echo "✓ .env file exists"
fi

# Run tests to verify installation
echo ""
echo "Running tests to verify installation..."
if python -m pytest tests/ -q --tb=no 2>/dev/null; then
    echo "✓ Tests passed"
else
    echo "⚠️  Some tests failed - this may be expected if database is not configured"
fi

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Edit .env with your API and database credentials"
echo "  2. Create the database: mysql -u root -p -e 'CREATE DATABASE real_estate'"
echo "  3. Run migrations: python scripts/migrate_schema.py"
echo "  4. Test the ETL: python -m trestle_etl.main --dry-run"
echo ""
echo "For more information, see docs/DEPLOYMENT.md"
