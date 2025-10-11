# Cascon Troubleshooting Guide

## Common Issues and Solutions

### 1. Cassandra Driver Installation Issues

#### Problem: 
```
Cassandra dependencies not fully available: Unable to load a default connection class
```

#### Solution:
Try these installation methods in order:

1. **Standard installation:**
   ```bash
   pip install cassandra-driver
   ```

2. **Without binary extensions (for compatibility):**
   ```bash
   pip install cassandra-driver --no-binary=cassandra-driver
   ```

3. **With environment variables (Windows):**
   ```cmd
   set CASS_DRIVER_NO_CYTHON=1
   set CASS_DRIVER_NO_LIBEV=1
   pip install cassandra-driver
   ```

4. **With environment variables (Linux/Mac):**
   ```bash
   CASS_DRIVER_NO_CYTHON=1 CASS_DRIVER_NO_LIBEV=1 pip install cassandra-driver
   ```

### 2. Python Version Compatibility

Cascon has been tested with Python 3.6-3.11. Python 3.12+ may have compatibility issues with the Cassandra driver due to removal of the `asyncore` module.

#### Solution:
Consider using Python 3.11 or earlier for full Cassandra functionality.

### 3. Network Connectivity Issues

#### Problem:
```
[ERROR] Cannot reach 192.168.1.8:9042
```

#### Solutions:
1. **Verify Cassandra is running:**
   ```bash
   # On Cassandra server
   sudo systemctl status cassandra
   # or
   ps aux | grep cassandra
   ```

2. **Check firewall settings:**
   ```bash
   # On Cassandra server, ensure port 9042 is open
   sudo ufw allow 9042
   ```

3. **Verify Cassandra configuration:**
   Check `cassandra.yaml` file:
   ```yaml
   # Ensure these settings are correct
   rpc_address: 0.0.0.0  # or specific IP
   broadcast_rpc_address: 192.168.1.8  # your server IP
   start_native_transport: true
   native_transport_port: 9042
   ```

4. **Test connectivity manually:**
   ```bash
   telnet 192.168.1.8 9042
   # or
   nc -zv 192.168.1.8 9042
   ```

### 4. Authentication Issues

#### Problem:
```
[ERROR] Connection failed: Authentication error
```

#### Solutions:
1. **Verify credentials:**
   Default Cassandra credentials are:
   - Username: cassandra
   - Password: cassandra

2. **Check if authentication is enabled:**
   In `cassandra.yaml`:
   ```yaml
   authenticator: PasswordAuthenticator
   authorizer: CassandraAuthorizer
   ```

3. **Create custom user (if needed):**
   Connect with cqlsh using default credentials and create a new user:
   ```sql
   CREATE USER your_username WITH PASSWORD 'your_password' SUPERUSER;
   ```

### 5. Keyspace/Schema Issues

#### Problem:
```
Error during setup: Keyspace creation failed
```

#### Solutions:
1. **Check permissions:**
   Ensure your user has CREATE permission:
   ```sql
   GRANT CREATE ON ALL KEYSPACES TO cassandra;
   ```

2. **Verify replication strategy:**
   For single-node clusters, use SimpleStrategy:
   ```sql
   CREATE KEYSPACE my_keyspace 
   WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
   ```

3. **For multi-node clusters, use NetworkTopologyStrategy:**
   ```sql
   CREATE KEYSPACE my_keyspace 
   WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};
   ```

### 6. Data Insertion Issues

#### Problem:
```
Error inserting data: [Some error]
```

#### Solutions:
1. **Check table schema:**
   Ensure column names and types match your CSV data.

2. **Verify sufficient resources:**
   Check disk space and memory on Cassandra node.

3. **Batch large inserts:**
   For large datasets, consider batching inserts:
   ```python
   # Process in chunks
   chunk_size = 1000
   for i in range(0, len(df), chunk_size):
       chunk = df.iloc[i:i+chunk_size]
       cascon.insert_dataframe(chunk, "table_name")
   ```

## Testing Your Setup

### 1. Network Test
```bash
python -m examples.check_connection
```

### 2. CSV Functionality Test
```bash
python -m examples.csv_only_demo
```

### 3. Full Integration Test
```bash
python -m examples.cassandra_example
```

## Alternative Approaches

### 1. Docker-based Cassandra
If you're having trouble with a remote Cassandra instance, try running locally:

```bash
docker run --name cassandra -p 9042:9042 -d cassandra:latest
```

Then update your connection parameters to:
- IP: localhost (127.0.0.1)
- Port: 9042
- Username: cassandra
- Password: cassandra

### 2. Using cqlsh Directly
To verify your Cassandra instance is working:

```bash
cqlsh 192.168.1.8 9042 -u cassandra -p cassandra
```

## Getting Help

If you continue to experience issues:

1. Check the Cassandra logs:
   ```bash
   # Usually located at:
   /var/log/cassandra/system.log
   # or
   $CASSANDRA_HOME/logs/system.log
   ```

2. Enable debug logging in Cascon:
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

3. Report issues with:
   - Python version
   - Cassandra version
   - Error messages
   - Steps to reproduce