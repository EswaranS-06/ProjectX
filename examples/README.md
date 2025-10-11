# Cascon Examples

This directory contains example scripts demonstrating various uses of the Cascon module.

## Scripts

### 1. cassandra_example.py
Main example showing how to:
- Connect to Cassandra
- Create keyspace and table
- Insert data from sample.csv
- Query the data

Usage:
```bash
python examples/cassandra_example.py
```

### 2. setup_sample_data.py
Complete script to set up the database with sample data:
- Creates keyspace `sample_keyspace`
- Creates table `sample_data`
- Inserts all data from sample.csv

Usage:
```bash
python examples/setup_sample_data.py
```

### 3. simple_sample_import.py
Simplified version focusing on essential operations:
- Connects to Cassandra at 192.168.1.8:9042
- Creates/uses keyspace `sample_keyspace`
- Creates table `log_data`
- Imports sample.csv data

Usage:
```bash
python examples/simple_sample_import.py
```

### 4. csv_only_demo.py
Demonstrates Cascon's CSV functionality when Cassandra is not available:
- Loads sample.csv data
- Shows data manipulation capabilities
- Converts to DataFrame for analysis

Usage:
```bash
python examples/csv_only_demo.py
```

### 5. check_connection.py
Utility to verify connectivity to your Cassandra instance:
- Checks network connectivity to 192.168.1.8:9042
- Attempts to establish Cassandra connection
- Lists available keyspaces if successful

Usage:
```bash
python examples/check_connection.py
```

## Prerequisites

1. Running Cassandra instance at 192.168.1.8:9042
2. sample.csv file in the project root directory
3. Installed dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

All scripts are pre-configured to connect to:
- Host: 192.168.1.8
- Port: 9042
- Username: cassandra
- Password: cassandra

Modify the Cascon initialization in each script if you need different connection parameters.

## Troubleshooting

If you encounter connection issues:
1. Run `check_connection.py` to diagnose connectivity problems
2. Ensure Cassandra is running on the target machine
3. Verify firewall settings allow connections on port 9042
4. Check that Cassandra is configured to accept remote connections
5. Confirm username/password credentials are correct