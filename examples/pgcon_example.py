"""
Example usage of the Pgcon (PostgreSQL Connector) module.
"""

from pgcon import Pgcon
import pandas as pd

def main():
    print("Pgcon Example - PostgreSQL Database Operations")
    print("=" * 50)
    
    # Initialize Pgcon with default parameters
    pgcon = Pgcon()
    
    # Demonstrate CSV functionality (always available)
    print("1. CSV Data Loading:")
    try:
        data = pgcon.load_csv_data("sample.csv")
        print(f"   Loaded {len(data)} rows from sample.csv")
        print(f"   First row keys: {list(data[0].keys())}")
    except FileNotFoundError:
        print("   sample.csv not found in current directory")
    
    print("\n2. Loading Specific Columns:")
    try:
        selected_data = pgcon.load_csv_data(
            "sample.csv", 
            columns=["event_count", "unique_messages", "ensemble_anomaly"]
        )
        print(f"   Loaded {len(selected_data)} rows with {len(selected_data[0])} columns")
        print(f"   Sample: {selected_data[0]}")
    except FileNotFoundError:
        print("   sample.csv not found in current directory")
    
    print("\n3. Converting to DataFrame:")
    try:
        data = pgcon.load_csv_data("sample.csv")
        df = pd.DataFrame(data)
        print(f"   DataFrame shape: {df.shape}")
        print(f"   Column names: {list(df.columns)}")
        print(f"   Anomaly distribution:\n{df['ensemble_anomaly'].value_counts()}")
    except FileNotFoundError:
        print("   sample.csv not found in current directory")
    
    # PostgreSQL functionality (when available)
    print("\n4. PostgreSQL Functionality:")
    print("   To use PostgreSQL database features:")
    print("   - Ensure psycopg2 is properly installed")
    print("   - Make sure PostgreSQL is running and accessible")
    print("   - Then you can use methods like:")
    print("     pgcon.connect()")
    print("     pgcon.execute_query('SELECT * FROM table_name LIMIT 5')")
    print("     pgcon.create_table_from_csv('sample.csv', 'logs_table')")
    print("     pgcon.insert_dataframe(df, 'logs_table')")
    print("     pgcon.close()")
    
    # Example with custom connection parameters
    print("\n5. Custom Connection Parameters:")
    pgcon_custom = Pgcon(
        host="192.168.1.10",
        port=5432,
        database="cybersecurity",
        username="analyst",
        password="securepassword"
    )
    print(f"   Configured for {pgcon_custom.host}:{pgcon_custom.port}/{pgcon_custom.database}")

if __name__ == "__main__":
    main()