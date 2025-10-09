#!/usr/bin/env python3
"""
Test script to verify felog module functionality
"""

import sys
import os

# Add the parent directory to the path so we can import felog
sys.path.insert(0, os.path.abspath('.'))

def test_imports():
    """Test that we can import the felog module and its classes."""
    try:
        import felog
        print("✓ Successfully imported felog module")
        
        # Test importing specific classes
        from felog import LogParser, FeatureEngineering
        print("✓ Successfully imported LogParser and FeatureEngineering")
        
        # Test creating instances
        parser = LogParser()
        print("✓ Successfully created LogParser instance")
        
        # Create a small DataFrame for testing FeatureEngineering
        import pandas as pd
        df = pd.DataFrame({
            'timestamp': pd.date_range('2025-01-01', periods=5, freq='1min'),
            'host': ['server1', 'server2', 'server1', 'server3', 'server2'],
            'process': ['sshd', 'cron', 'sshd', 'kernel', 'cron'],
            'pid': [1234, 5678, 1235, 1, 5679],
            'message': ['Login successful', 'Job completed', 'Login failed', 'System boot', 'Job started'],
            'src_ip': ['192.168.1.1', None, '192.168.1.2', None, None],
            'user': ['alice', None, 'bob', None, None]
        })
        
        fe = FeatureEngineering(df, window_seconds=300)  # 5-minute windows
        print("✓ Successfully created FeatureEngineering instance")
        
        # Test getting features (should work even with small dataset)
        features = fe.get_features()
        print(f"✓ Successfully generated features (shape: {features.shape})")
        
        print("\n🎉 All tests passed! The felog module is working correctly.")
        return True
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

if __name__ == "__main__":
    test_imports()