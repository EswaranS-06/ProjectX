# Pgcon (PostgreSQL Connector)

Pgcon is a Python module designed to simplify interactions with PostgreSQL databases. It provides a high-level interface for common operations while maintaining flexibility for more complex use cases.

**Note:** The module currently provides robust CSV processing capabilities and can be extended with full PostgreSQL support when the driver is properly configured.

## Features

- Easy connection management to PostgreSQL databases (when available)
- SQL query execution (when available)
- CSV file data insertion (when available)
- Pandas DataFrame insertion (when available)
- **CSV data processing** (always available)

## Installation

Ensure you have the required dependencies installed:

```bash
pip install -r requirements.txt
```

Required dependencies include:
- pandas
- numpy
- psycopg2 (for PostgreSQL functionality)

### Installing PostgreSQL Driver

To enable full PostgreSQL functionality, install the psycopg2 package:

```bash
# Standard installation
pip install psycopg2

# If you encounter compilation issues, try the binary version
pip install psycopg2-binary
```

If you continue to have issues, the module will work in CSV-only mode.

## Usage

### Importing the Module

```python
# Correct way to import Pgcon
from pgcon import Pgcon
```

### Initialization

```python
# Initialize with default parameters
pgcon = Pgcon()

# Or initialize with custom parameters
pgcon = Pgcon(
    host="127.0.0.1",
    port=5432,
    database="postgres",
    username="postgres",
    password="postgres"
)
```

### Loading CSV Data (Always Available)

```python
# Load all data from CSV
data = pgcon.load_csv_data("path/to/your/file.csv")

# Load specific columns from CSV
data = pgcon.load_csv_data("path/to/your/file.csv", columns=["col1", "col2"])
```

### Converting to DataFrame

```python
# Load data and convert to pandas DataFrame
data = pgcon.load_csv_data("path/to/your/file.csv")
df = pd.DataFrame(data)
```

### Connecting to PostgreSQL (When Available)

```python
pgcon.connect()
```

### Executing SQL Queries (When Available)

```python
# Execute SELECT query
results = pgcon.execute_query("SELECT * FROM table_name LIMIT 10;")

# Execute INSERT/UPDATE/DELETE query
pgcon.execute_query("INSERT INTO table_name (col1, col2) VALUES (%s, %s);", ("value1", "value2"))
```

### Creating Tables from CSV (When Available)

```python
# Automatically create table based on CSV structure and insert data
pgcon.create_table_from_csv("path/to/your/file.csv", "table_name")
```

### Inserting Data from Pandas DataFrame (When Available)

```python
import pandas as pd

df = pd.read_csv("path/to/your/file.csv")
pgcon.insert_dataframe(df, "table_name")
```

### Closing Connection (When Available)

```python
pgcon.close()
```

## Example Usage

```python
from pgcon import Pgcon

# Create instance
pgcon = Pgcon(host="192.168.1.10", port=5432, database="mydb")

# Load CSV data (always available)
data = pgcon.load_csv_data("sample.csv")
print(f"Loaded {len(data)} rows")

# When PostgreSQL is available:
# pgcon.connect()
# pgcon.execute_query("CREATE TABLE IF NOT EXISTS logs (id SERIAL PRIMARY KEY, message TEXT);")
# pgcon.insert_dataframe(pd.DataFrame(data), "logs")
# pgcon.close()
```

## Files in this Module

- `pgcon/__init__.py` - Package initialization (enables `from pgcon import Pgcon`)
- `pgcon/postgresql_connector.py` - Main Pgcon class implementation
- `pgcon/README.md` - This documentation

## Troubleshooting

If you encounter issues with the PostgreSQL driver:

1. Try installing `psycopg2-binary` instead of `psycopg2`
2. Ensure PostgreSQL is running and accessible
3. Verify connection parameters (host, port, database, username, password)
4. Consider using the module in CSV-only mode for data processing tasks

## License

This project is licensed under the MIT License.