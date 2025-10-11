# Pgcon Examples

This directory contains example scripts demonstrating various uses of the Pgcon module.

## Scripts

### 1. pgcon_example.py
Main example showing how to:
- Load and process CSV data
- Use Pgcon with custom connection parameters
- Demonstrate available methods

Usage:
```bash
python examples/pgcon_example.py
```

## Prerequisites

1. For CSV functionality (always available):
   - sample.csv file in the project root directory
   - pandas and numpy libraries

2. For PostgreSQL functionality:
   - Running PostgreSQL instance
   - Installed dependencies:
     ```bash
     pip install psycopg2-binary
     ```

## Configuration

The example script is pre-configured with default parameters but shows how to customize:
- Host: PostgreSQL server address
- Port: PostgreSQL port (default 5432)
- Database: Target database name
- Username: Database user
- Password: Database password

## Troubleshooting

If you encounter issues:
1. Ensure sample.csv exists in the project root
2. For PostgreSQL functionality:
   - Verify PostgreSQL is running
   - Check connection parameters
   - Confirm psycopg2-binary is installed
   - Test connectivity with psql command line tool