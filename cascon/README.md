# Cascon (Cassandra Connector)

Cascon is a Python module designed to simplify interactions with Apache Cassandra databases. It provides a high-level interface for common operations while maintaining flexibility for more complex use cases.

## Features

- Easy connection management to Cassandra clusters
- Keyspace selection
- CSV file data insertion
- Pandas DataFrame insertion
- Raw CQL command execution

## Installation

Ensure you have the required dependencies installed:

```bash
pip install -r requirements.txt
```

Required dependencies include:
- cassandra-driver
- pandas
- numpy

## Usage

### Initialization

```python
from cascon.cassandra_connector import Cascon

# Initialize with default parameters
cascon = Cascon()

# Or initialize with custom parameters
cascon = Cascon(
    ip="127.0.0.1",
    port=9042,
    username="cassandra",
    password="cassandra"
)
```

### Connecting to Cassandra

```python
cascon.connect()
```

### Setting Keyspace

```python
cascon.set_keyspace("your_keyspace_name")
```

### Inserting Data from CSV

```python
cascon.insert_from_csv("path/to/your/file.csv", "table_name")
```

### Inserting Data from Pandas DataFrame

```python
import pandas as pd

df = pd.read_csv("path/to/your/file.csv")
cascon.insert_dataframe(df, "table_name")
```

### Executing Raw CQL Commands

```python
result = cascon.cqlsh("SELECT * FROM table_name LIMIT 10;")
print(result)
```

### Closing Connection

```python
cascon.close()
```

## Example

See `examples/cassandra_example.py` for a complete working example.

## License

This project is licensed under the MIT License.