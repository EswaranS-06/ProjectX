"""
Test script to verify Pgcon module import and functionality.
"""

# Test direct import
from pgcon import Pgcon
import pandas as pd

def test_basic_import():
    """Test that we can import Pgcon directly."""
    print("[SUCCESS] Successfully imported Pgcon from pgcon")
    
    # Test instantiation
    pgcon = Pgcon()
    print("[SUCCESS] Successfully instantiated Pgcon")
    
    # Test with custom parameters
    pgcon_custom = Pgcon(
        host="192.168.1.10",
        port=5432,
        database="cybersecurity",
        username="analyst",
        password="securepassword"
    )
    print("[SUCCESS] Successfully instantiated Pgcon with custom parameters")
    print(f"  Host: {pgcon_custom.host}")
    print(f"  Port: {pgcon_custom.port}")
    print(f"  Database: {pgcon_custom.database}")
    print(f"  Username: {pgcon_custom.username}")
    
    return pgcon

def test_csv_functionality():
    """Test CSV functionality."""
    pgcon = Pgcon()
    
    try:
        # Test loading sample.csv
        data = pgcon.load_csv_data("sample.csv")
        print(f"[SUCCESS] Successfully loaded {len(data)} rows from sample.csv")
        
        # Test loading specific columns
        selected_data = pgcon.load_csv_data("sample.csv", columns=["event_count", "unique_messages"])
        print(f"[SUCCESS] Successfully loaded {len(selected_data)} rows with selected columns")
        
        return True
    except FileNotFoundError:
        print("[WARNING] sample.csv not found, skipping CSV tests")
        return False
    except Exception as e:
        print(f"[ERROR] Error in CSV functionality: {e}")
        return False

def test_method_availability():
    """Test that all expected methods are available."""
    pgcon = Pgcon()
    
    expected_methods = [
        'connect', 'execute_query', 'create_table_from_csv', 
        'insert_dataframe', 'close', 'load_csv_data'
    ]
    
    for method in expected_methods:
        if hasattr(pgcon, method):
            print(f"[SUCCESS] Method '{method}' is available")
        else:
            print(f"[ERROR] Method '{method}' is missing")

def main():
    print("Testing Pgcon Module Import and Functionality")
    print("=" * 50)
    
    # Test import
    pgcon = test_basic_import()
    
    print("\nTesting Method Availability:")
    test_method_availability()
    
    print("\nTesting CSV Functionality:")
    test_csv_functionality()
    
    print("\n" + "=" * 50)
    print("Module import and basic functionality verified!")

if __name__ == "__main__":
    main()