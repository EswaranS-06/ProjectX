"""
Utility script to check Cassandra connection.
"""

from cascon.cassandra_connector import Cascon
import socket

def check_network_connectivity(host, port):
    """Check if a host:port is reachable."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False

def main():
    HOST = "192.168.1.8"
    PORT = 9042
    
    print(f"Checking connectivity to {HOST}:{PORT}...")
    
    # Check network connectivity first
    if not check_network_connectivity(HOST, PORT):
        print(f"[ERROR] Cannot reach {HOST}:{PORT}")
        print("Please ensure:")
        print("1. Cassandra is running on the target machine")
        print("2. The IP address is correct")
        print("3. Port 9042 is open and not blocked by firewall")
        return
    
    print(f"[SUCCESS] Network connectivity to {HOST}:{PORT} established")
    
    # Try to connect with Cascon
    print("Attempting Cassandra connection...")
    cascon = Cascon(
        ip=HOST,
        port=PORT,
        username="cassandra",
        password="cassandra"
    )
    
    try:
        cascon.connect()
        print("[SUCCESS] Successfully connected to Cassandra!")
        
        # List keyspaces
        try:
            result = cascon.cqlsh("DESCRIBE KEYSPACES;")
            print("Available keyspaces:")
            for row in result:
                print(f"  - {row}")
        except Exception as e:
            print(f"Could not list keyspaces: {e}")
            
    except ImportError:
        print("[ERROR] Cassandra driver not properly installed")
        print("Try: pip install cassandra-driver")
    except Exception as e:
        print(f"[ERROR] Connection failed: {e}")
        print("Please check:")
        print("1. Cassandra credentials (username/password)")
        print("2. Cassandra configuration allows remote connections")
        print("3. cassandra.yaml has rpc_address set appropriately")
    finally:
        try:
            cascon.close()
        except:
            pass

if __name__ == "__main__":
    main()