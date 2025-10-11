"""
Simple script to import sample.csv data using Cascon.
"""

from cascon.cassandra_connector import Cascon
import pandas as pd
import uuid

def main():
    # Initialize Cascon with your parameters
    print("Initializing Cascon...")
    cascon = Cascon(
        ip="192.168.1.8",
        port=9042
        # Using default username/password: cassandra/cassandra
    )
    
    try:
        # Connect to Cassandra
        print("Connecting to Cassandra at 192.168.1.8:9042...")
        cascon.connect()
        
        # Create and use keyspace
        print("Creating/using keyspace...")
        try:
            cascon.cqlsh("CREATE KEYSPACE sample_keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
        except Exception as e:
            print(f"Keyspace creation warning (may already exist): {e}")
        
        cascon.set_keyspace("sample_keyspace")
        
        # Create table
        print("Creating table...")
        create_table_cql = """
            CREATE TABLE IF NOT EXISTS log_data (
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
        cascon.cqlsh(create_table_cql)
        
        # Load and prepare data
        print("Loading sample.csv data...")
        df = pd.read_csv("sample.csv")
        
        # Add UUID for primary key
        df['id'] = [uuid.uuid4() for _ in range(len(df))]
        
        # Insert data
        print(f"Inserting {len(df)} records...")
        cascon.insert_dataframe(df, "log_data")
        
        # Verify
        result = cascon.cqlsh("SELECT COUNT(*) FROM log_data;")
        print(f"Successfully inserted data. Total records: {result[0]['count']}")
        
        print("Sample records:")
        samples = cascon.cqlsh("SELECT event_count, unique_messages, avg_msg_length FROM log_data LIMIT 3;")
        for sample in samples:
            print(f"  - Events: {sample['event_count']}, Messages: {sample['unique_messages']}, Avg Length: {sample['avg_msg_length']:.2f}")
        
    except ImportError as e:
        print(f"Cassandra functionality not available: {e}")
        print("The cassandra-driver package is required for database operations.")
        print("You can still use Cascon for CSV processing.")
    except Exception as e:
        print(f"Error: {e}")
        
    finally:
        try:
            cascon.close()
            print("Connection closed.")
        except:
            pass  # Ignore errors when closing

if __name__ == "__main__":
    main()