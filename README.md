
# Feature Engineering Logs Module (felog)

A Python module for parsing cyber security logs and extracting ML-ready features.

## Features

- Ingests logs from multiple sources:
  - Single file
  - Folder of log files
  - UDP port 514 (syslog)
- Parses logs into a normalized schema
- Extracts time-window aggregated features
- Returns Pandas DataFrame for ML workflows
- No intermediate file storage
- Detailed logging capabilities

## Installation

### Creating a Virtual Environment

It's recommended to use a virtual environment to avoid conflicts with other Python packages:

```bash
# Create a virtual environment
python -m venv felog_env

# Activate the virtual environment
# On Windows:
felog_env\Scripts\activate
# On macOS/Linux:
source felog_env/bin/activate
```

### Installing Requirements

```bash
# Install required packages
pip install -r requirements.txt
```

Required packages:
- pandas>=1.3.0
- numpy>=1.21.0
- python-dateutil>=2.8.0

## Usage

### Basic Usage

```python
from felog import LogParser, FeatureEngineering

# Parse logs from folder
parser = LogParser()
parser.from_folder("/var/logs/syslog_folder")
df_parsed = parser.normalize()

# Feature engineering
fe = FeatureEngineering(df_parsed, window_seconds=60)
features_df = fe.get_features()

# Optionally save features
fe.save_csv("features.csv")
fe.save_json("features.json")
```

### Parsing from a Single File

```python
parser = LogParser()
parser.from_file("/var/log/auth.log")
df_parsed = parser.normalize()
```

### Parsing from UDP Port (Syslog)

```python
parser = LogParser()
parser.from_udp_port(host='0.0.0.0', port=514, max_logs=1000)
df_parsed = parser.normalize()
```

## Normalized Fields

The parser extracts the following fields when possible:
- `timestamp`: Event timestamp
- `host`: Hostname where event occurred
- `process`: Process name
- `pid`: Process ID
- `message`: Log message
- `src_ip`: Source IP address (extracted from message)
- `user`: User involved (extracted from message)

## Feature Engineering

The feature engineering module computes the following features per time window:
- `event_count`: Number of events
- `unique_messages`: Number of unique messages
- `distinct_hosts`: Number of distinct hosts
- `distinct_processes`: Number of distinct processes
- `avg_msg_length`: Average message length
- `failed_auth_count`: Count of failed authentication attempts
- `invalid_user_count`: Count of invalid user attempts
- `entropy_tokens`: Entropy of message tokens

## Detailed Documentation

For detailed documentation about the implementation, please see the [internal documentation](felog/README.md).

## Requirements

- pandas>=1.3.0
- numpy>=1.21.0
- python-dateutil>=2.8.0

## License

MIT