"""
Script to use Cascon to connect to Cassandra, create keyspace/table, and insert sample data.
"""

from cascon.cassandra_connector import Cascon
import pandas as pd
import sys
import uuid

def main():
    # Initialize Cascon with your specified parameters
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
        cascon.cqlsh("""
            CREATE KEYSPACE IF NOT EXISTS sample_keyspace 
            WITH REPLICATION = {
                'class': 'SimpleStrategy', 
                'replication_factor': 1
            }
        """)
        
        # Use the keyspace
        print("Setting keyspace...")
        cascon.set_keyspace("sample_keyspace")
        
        # Create table based on sample.csv structure
        print("Creating table...")
        cascon.cqlsh("""
            CREATE TABLE IF NOT EXISTS sample_data (
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
        """)
        
        # Load data from CSV
        print("Loading data from sample.csv...")
        df = pd.read_csv("sample.csv")
        
        # Add an ID column for the primary key
        df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        # Insert data using the dataframe method
        print(f"Inserting {len(df)} rows into sample_data table...")
        cascon.insert_dataframe(df, "sample_data")
        
        print("Data insertion completed successfully!")
        
        # Verify data was inserted
        print("Verifying data...")
        result = cascon.cqlsh("SELECT COUNT(*) FROM sample_data;")
        print(f"Total rows in table: {result[0]['count']}")
        
        # Show first few rows
        print("Sample data:")
        result = cascon.cqlsh("SELECT * FROM sample_data LIMIT 3;")
        for row in result:
            print(row)
            
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
        
    finally:
        # Close the connection
        print("Closing connection...")
        cascon.close()
        print("Done!")

if __name__ == "__main__":
    main()