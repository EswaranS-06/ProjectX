#!/usr/bin/env python3
"""
Example usage of the Feature Engineering Logs module (felog)
"""

import sys
import os

# Add the parent directory to the path so we can import felog
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from felog import LogParser, FeatureEngineering


def main():

    

    # Step 1: Parse logs from file
    print("\n1. PARSING LOGS FROM FILE...")
    print("-" * 30)
    parser = LogParser(enable_logging=True)
    parser.from_file("D:\ProjectX\logs\Linux_2k.log".encode())
    df_parsed = parser.normalize()
  
    
    # Step 2: Feature engineering
    print("\n\n2. GENERATING FEATURES...")
    print("-" * 25)
    fe = FeatureEngineering(df_parsed, window_seconds=60, enable_logging=True)
    features_df = fe.get_features()
    
    print(f"\nGenerated {len(features_df)} feature rows")
    print("\nFeatures:")
    print(features_df.to_string())
    
    # Step 3: Save features
    print("\n\n3. SAVING FEATURES...")
    print("-" * 20)
    fe.save_csv("features.csv")
    fe.save_json("features.json")
    print("\nSaved features to features.csv and features.json")
    
    # Show log files created
    print("\n\n4. LOG FILES CREATED...")
    print("-" * 22)
    try:
        log_files = [f for f in os.listdir("tmp") if f.endswith(".log")]
        for log_file in sorted(log_files):
            print(f"  - tmp/{log_file}")
    except Exception as e:
        print(f"  Could not list log files: {e}")
    
    # Cleanup
    try:
        os.remove("sample.log")
        print("\nCleaned up sample.log")
    except:
        pass
    
    print("\n" + "=" * 60)
    print("PROCESSING COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()