# Cyber Log Feature Engineering Module - Internal Documentation

This folder contains the core implementation of the Cyber Log Feature Engineering module. Below is a detailed explanation of each file, class, and function.

## File Structure

1. `__init__.py` - Package initialization file
2. `parser.py` - Log parsing functionality
3. `feature_engineering.py` - Feature extraction and engineering

## Detailed Component Documentation

### `__init__.py`

This file initializes the package and makes the main classes available for import. It exports:
- `LogParser` from `parser.py`
- `FeatureEngineering` from `feature_engineering.py`

This allows users to import the classes directly:
```python
from cyber_log_fe import LogParser, FeatureEngineering
```

### `parser.py`

This file contains the `LogParser` class responsible for ingesting logs from various sources and normalizing them into a consistent format.

#### Class: `LogParser`

##### Purpose
The `LogParser` class handles ingestion of raw logs from multiple sources and converts them into a normalized pandas DataFrame with consistent fields.

##### Constructor: `__init__(self)`
Initializes the parser with an empty list to store raw logs.
- `self.raw_logs`: List to store raw log lines before normalization

##### Method: `from_file(self, file_path: str)`
Reads log entries from a single file.

**Parameters:**
- `file_path` (str): Path to the log file to read

**Returns:**
- `self`: Returns the instance for method chaining

**Implementation Details:**
- Opens the file with UTF-8 encoding and ignores decoding errors
- Reads all non-empty lines and strips whitespace
- Stores the lines in `self.raw_logs`
- Uses `errors='ignore'` to handle potential encoding issues

##### Method: `from_folder(self, folder_path: str)`
Reads log entries from all files in a specified folder.

**Parameters:**
- `folder_path` (str): Path to the folder containing log files

**Returns:**
- `self`: Returns the instance for method chaining

**Implementation Details:**
- Iterates through all items in the folder using `os.listdir()`
- Checks if each item is a file (not a subdirectory)
- Reads each file and extends the logs list with its contents
- Strips whitespace from each line and filters out empty lines
- Stores all collected logs in `self.raw_logs`

##### Method: `from_udp_port(self, host='0.0.0.0', port=514, max_logs=1000)`
Listens for log entries on a UDP port (typically used for syslog).

**Parameters:**
- `host` (str): Host address to bind to (default: '0.0.0.0' for all interfaces)
- `port` (int): Port number to listen on (default: 514 for syslog)
- `max_logs` (int): Maximum number of logs to receive before stopping (default: 1000)

**Returns:**
- `self`: Returns the instance for method chaining

**Implementation Details:**
- Creates a UDP socket using `socket.socket()`
- Binds the socket to the specified host and port
- Receives up to `max_logs` messages
- Decodes received bytes to UTF-8 strings, ignoring errors
- Strips whitespace from each message
- Properly closes the socket in a finally block to ensure cleanup
- Stores received logs in `self.raw_logs`

##### Method: `normalize(self) -> pd.DataFrame`
Converts raw log lines into a normalized pandas DataFrame with consistent fields.

**Returns:**
- `pd.DataFrame`: DataFrame with normalized log fields

**Normalized Fields:**
- `timestamp`: Datetime object representing when the event occurred
- `host`: Hostname where the event occurred
- `process`: Name of the process that generated the log
- `pid`: Process ID (integer)
- `message`: The main log message content
- `src_ip`: Source IP address extracted from the message (if present)
- `user`: Username involved in the event (if present)

**Implementation Details:**
- Uses regex pattern `^(\w+\s+\d+\s+\d+:\d+:\d+)\s+(\S+)\s+(\S+)\[(\d+)\]:\s+(.*)$` to parse standard syslog format
- Attempts to parse timestamps using `dateutil.parser.parse()`
- Extracts IP addresses using pattern `(\d{1,3}(?:\.\d{1,3}){3})`
- Extracts usernames using pattern `user (\S+)`
- Handles lines that don't match the expected format by preserving the full line in the message field
- Converts PID to integer when possible
- Returns a pandas DataFrame with all parsed information

### `feature_engineering.py`

This file contains the `FeatureEngineering` class responsible for converting normalized logs into ML-ready features using time-window aggregation.

#### Class: `FeatureEngineering`

##### Purpose
The `FeatureEngineering` class takes normalized log data and extracts time-series features suitable for machine learning models. It aggregates log events into time windows and computes statistical measures.

##### Constructor: `__init__(self, df_parsed: pd.DataFrame, window_seconds=60)`
Initializes the feature engineering processor with parsed log data.

**Parameters:**
- `df_parsed` (pd.DataFrame): Normalized log data from `LogParser.normalize()`
- `window_seconds` (int): Size of time windows for feature aggregation (default: 60 seconds)

**Attributes:**
- `self.df`: Stores the parsed DataFrame
- `self.window`: Stores the window size in seconds

##### Method: `_calculate_entropy(self, messages: List[str]) -> float`
Calculates the entropy of tokens in a collection of messages, measuring randomness/unpredictability.

**Parameters:**
- `messages` (List[str]): List of message strings to analyze

**Returns:**
- `float`: Entropy value (0.0 for completely predictable text, higher for more random text)

**Implementation Details:**
- Tokenizes each message into words using regex pattern `\w+`
- Converts tokens to lowercase for consistency
- Counts token occurrences using `collections.Counter`
- Calculates probability of each token
- Applies entropy formula: `-sum(p * log2(p))`
- Returns 0.0 for empty message collections

##### Method: `get_features(self) -> pd.DataFrame`
Generates time-window aggregated features from the parsed log data.

**Returns:**
- `pd.DataFrame`: DataFrame with one row per time window and columns for each feature

**Time Window Processing:**
- Filters out logs with missing timestamps
- Sorts logs chronologically
- Creates fixed-size windows based on `window_seconds`
- Processes each window separately

**Computed Features:**
- `window_start`: Start time of the window
- `window_end`: End time of the window
- `event_count`: Total number of log events in the window
- `unique_messages`: Number of distinct log messages
- `distinct_hosts`: Number of distinct hosts with events
- `distinct_processes`: Number of distinct processes with events
- `avg_msg_length`: Average length of log messages
- `failed_auth_count`: Count of messages containing "failed password"
- `invalid_user_count`: Count of messages containing "invalid user"
- `entropy_tokens`: Entropy of all tokens in messages (measure of randomness)

**Implementation Details:**
- Skips empty windows to reduce noise
- Uses pandas string methods with regex for pattern matching
- Handles edge cases like empty DataFrames gracefully
- Returns DataFrame with consistent column structure even when empty

##### Method: `save_csv(self, file_path: str)`
Exports computed features to a CSV file.

**Parameters:**
- `file_path` (str): Path where the CSV file should be saved

**Implementation Details:**
- Calls `get_features()` to compute features
- Uses pandas `to_csv()` with `index=False` to avoid writing row indices

##### Method: `save_json(self, file_path: str)`
Exports computed features to a JSON file.

**Parameters:**
- `file_path` (str): Path where the JSON file should be saved

**Implementation Details:**
- Calls `get_features()` to compute features
- Uses pandas `to_json()` with `orient='records'` for array of objects format
- Uses `date_format='iso'` for standardized timestamp representation

## Usage Flow

1. **Initialize Parser**: Create a `LogParser` instance
2. **Load Data**: Use one of the input methods (`from_file`, `from_folder`, `from_udp_port`)
3. **Normalize**: Call `normalize()` to get a structured DataFrame
4. **Initialize Feature Engine**: Create a `FeatureEngineering` instance with the parsed data
5. **Extract Features**: Call `get_features()` to compute ML-ready features
6. **Export Results**: Optionally save features using `save_csv()` or `save_json()`

## Error Handling

- File operations include proper error handling for missing files or permissions
- Network operations have timeouts and proper resource cleanup
- Data parsing gracefully handles malformed log entries
- Empty inputs produce empty but correctly structured outputs
- Encoding issues are handled with error-tolerant decoding