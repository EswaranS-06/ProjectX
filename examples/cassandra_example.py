"""
Example usage of the Cascon (Cassandra Connector) module.
"""

from cascon.cassandra_connector import Cascon
import pandas as pd
import uuid

def setup_sample_database():
    """Set up keyspace and table for sample data."""
    # Initialize Cascon with your parameters
    cascon = Cascon(
        ip="192.168.1.8",
        port=9042,
        username="cassandra",
        password="cassandra"
    )
    
    try:
        print("Connecting to Cassandra...")
        cascon.connect()
        
        # Create keyspace
        print("Creating keyspace...")
        try:
            cascon.cqlsh("""
                CREATE KEYSPACE IF NOT EXISTS sample_analysis 
                WITH REPLICATION = {
                    'class': 'SimpleStrategy', 
                    'replication_factor': 1
                }
            """)
            print("Keyspace 'sample_analysis' created successfully!")
        except Exception as e:
            print(f"Note: {e}")
        
        # Use the keyspace
        print("Setting keyspace...")
        cascon.set_keyspace("sample_analysis")
        
        # Create table based on sample.csv structure
        print("Creating table...")
        create_table_query = """
            CREATE TABLE IF NOT EXISTS security_logs (
                id UUID PRIMARY KEY,
                event_count int,
                unique_messages int,
                distinct_hosts int,
                distinct_processes int,
                avg_msg_length double,
                failed_auth_count int,
                invalid_user_count int,
                entropy_tokens double,
                isolation_forest_label int,
                isolation_forest_score double,
                one_class_svm_label int,
                one_class_svm_score double,
                dbscan_label int,
                dbscan_score double,
                ensemble_anomaly int
            )
        """
        cascon.cqlsh(create_table_query)
        print("Table 'security_logs' created successfully!")
        
        return cascon
        
    except ImportError:
        print("Cassandra functionality not available.")
        print("Install cassandra-driver for full database connectivity.")
        return None
    except Exception as e:
        print(f"Error during setup: {e}")
        return None

def insert_sample_data(cascon):
    """Insert sample data from CSV file."""
    if not cascon:
        return
        
    try:
        # Load data from CSV
        print("Loading data from sample.csv...")
        df = pd.read_csv("sample.csv")
        print(f"Loaded {len(df)} rows from sample.csv")
        
        # Add an ID column for the primary key
        df['id'] = [uuid.uuid4() for _ in range(len(df))]
        
        # Insert data
        print("Inserting data into security_logs table...")
        cascon.insert_dataframe(df, "security_logs")
        print("Data insertion completed!")
        
        # Verify insertion
        result = cascon.cqlsh("SELECT COUNT(*) FROM security_logs;")
        print(f"Total records in database: {result[0]['count']}")
        
    except FileNotFoundError:
        print("sample.csv not found. Please ensure the file exists.")
    except Exception as e:
        print(f"Error inserting data: {e}")

def query_sample_data(cascon):
    """Perform sample queries on the data."""
    if not cascon:
        return
        
    try:
        print("Performing sample queries...")
        
        # Count total records
        result = cascon.cqlsh("SELECT COUNT(*) FROM security_logs;")
        print(f"Total records: {result[0]['count']}")
        
        # Show some sample data
        print("Sample records:")
        result = cascon.cqlsh("""
            SELECT event_count, unique_messages, avg_msg_length, ensemble_anomaly 
            FROM security_logs 
            LIMIT 5;
        """)
        for i, row in enumerate(result, 1):
            print(f"  {i}. Events: {row['event_count']}, Messages: {row['unique_messages']}, "
                  f"Avg Length: {row['avg_msg_length']:.2f}, Anomaly: {row['ensemble_anomaly']}")
        
        # Count anomalies
        result = cascon.cqlsh("""
            SELECT COUNT(*) FROM security_logs WHERE ensemble_anomaly = 1 ALLOW FILTERING;
        """)
        anomaly_count = result[0]['count']
        print(f"Anomalous events: {anomaly_count}")
        
    except Exception as e:
        print(f"Error querying data: {e}")

def main():
    print("Cascon Example - Cassandra Database Operations")
    print("=" * 50)
    
    # Set up database
    cascon = setup_sample_database()
    
    if cascon:
        try:
            # Insert data
            insert_sample_data(cascon)
            
            # Query data
            query_sample_data(cascon)
            
        finally:
            # Close connection
            print("Closing connection...")
            cascon.close()
            print("Connection closed.")
    else:
        print("Running in CSV-only mode...")
        # Demo CSV functionality
        cascon = Cascon()
        try:
            data = cascon.load_csv_data("sample.csv")
            print(f"Loaded {len(data)} rows from sample.csv")
            print("First row:", data[0])
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()