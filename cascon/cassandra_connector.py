import pandas as pd
from typing import List, Dict, Any, Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global flag to track Cassandra availability
CASSANDRA_AVAILABLE = False

# Try to import Cassandra dependencies
try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    # Try to create a minimal cluster to test if the driver works
    CASSANDRA_AVAILABLE = True
    logger.info("Cassandra dependencies successfully imported")
except Exception as e:
    logger.warning(f"Cassandra dependencies not fully available: {e}")
    logger.warning("Cassandra functionality will be limited")
    Cluster = None
    PlainTextAuthProvider = None

class Cascon:
    """
    Cascon (Cassandra Connector) - A module to handle Cassandra database connections
    and data operations. For now, handles CSV data processing as a starting point.
    """
    
    def __init__(self, ip: str = "127.0.0.1", port: int = 9042, 
                 username: str = "cassandra", password: str = "cassandra"):
        """
        Initialize the Cascon instance with Cassandra connection parameters.
        
        Args:
            ip (str): IP address of the Cassandra cluster. Defaults to "127.0.0.1".
            port (int): Port number for Cassandra connection. Defaults to 9042.
            username (str): Username for authentication. Defaults to "cassandra".
            password (str): Password for authentication. Defaults to "cassandra".
        """
        self.ip = ip
        self.port = port
        self.username = username
        self.password = password
        self.cluster = None
        self.session = None
        self.keyspace = None
        self.data = None
        
        if not CASSANDRA_AVAILABLE:
            logger.info("Running in CSV-only mode. Install cassandra-driver for full functionality.")
    
    def _check_cassandra_availability(self):
        """Check if Cassandra dependencies are available."""
        if not CASSANDRA_AVAILABLE:
            raise ImportError(
                "Cassandra dependencies not available. "
                "Please install cassandra-driver package for full functionality. "
                "Current version supports CSV processing only."
            )
    
    def load_csv_data(self, file_path: str, columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Load data from CSV file.
        
        Args:
            file_path (str): Path to the CSV file
            columns (Optional[List[str]]): List of specific columns to load. 
                                          If None, loads all columns.
        
        Returns:
            List[Dict[str, Any]]: List of rows as dictionaries
        """
        try:
            # Read CSV file
            if columns:
                # Read only specified columns
                df = pd.read_csv(file_path, usecols=columns)
            else:
                # Read all columns
                df = pd.read_csv(file_path)
            
            # Convert to list of dictionaries
            self.data = df.to_dict('records')
            
            logger.info(f"Successfully loaded {len(self.data)} rows from {file_path}")
            if columns:
                logger.info(f"Loaded columns: {columns}")
            else:
                logger.info(f"Loaded columns: {list(df.columns)}")
                
            return self.data
            
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"Error loading CSV data: {str(e)}")
            raise
    
    def connect(self) -> None:
        """
        Establish connection to the Cassandra cluster.
        """
        self._check_cassandra_availability()
        
        try:
            auth_provider = PlainTextAuthProvider(username=self.username, password=self.password)
            self.cluster = Cluster([self.ip], port=self.port, auth_provider=auth_provider)
            self.session = self.cluster.connect()
            logger.info(f"Connected to Cassandra cluster at {self.ip}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {str(e)}")
            raise
    
    def set_keyspace(self, keyspace: str) -> None:
        """
        Set the keyspace for subsequent operations.
        
        Args:
            keyspace (str): Name of the keyspace to use.
        """
        self._check_cassandra_availability()
        
        if not self.session:
            self.connect()
            
        try:
            self.session.set_keyspace(keyspace)
            self.keyspace = keyspace
            logger.info(f"Using keyspace: {keyspace}")
        except Exception as e:
            logger.error(f"Failed to set keyspace {keyspace}: {str(e)}")
            raise
    
    def insert_from_csv(self, file_path: str, table: str, 
                       columns: Optional[List[str]] = None) -> None:
        """
        Insert data from a CSV file into a Cassandra table.
        
        Args:
            file_path (str): Path to the CSV file.
            table (str): Name of the target table.
            columns (Optional[List[str]]): List of specific columns to load. 
                                          If None, loads all columns.
        """
        self._check_cassandra_availability()
        
        if not self.session:
            self.connect()
            
        try:
            # Load CSV data
            if columns:
                df = pd.read_csv(file_path, usecols=columns)
            else:
                df = pd.read_csv(file_path)
            
            # Prepare insert statement
            column_names = list(df.columns)
            placeholders = ", ".join(["?" for _ in column_names])
            columns_str = ", ".join(column_names)
            insert_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
            
            # Insert each row
            prepared = self.session.prepare(insert_query)
            for _, row in df.iterrows():
                values = [row[col] for col in column_names]
                self.session.execute(prepared, values)
                
            logger.info(f"Successfully inserted {len(df)} rows from {file_path} into {table}")
            
        except Exception as e:
            logger.error(f"Failed to insert data from CSV: {str(e)}")
            raise
    
    def insert_dataframe(self, df: pd.DataFrame, table: str) -> None:
        """
        Insert data from a pandas DataFrame into a Cassandra table.
        
        Args:
            df (pd.DataFrame): DataFrame containing the data to insert.
            table (str): Name of the target table.
        """
        self._check_cassandra_availability()
        
        if not self.session:
            self.connect()
            
        try:
            # Prepare insert statement
            column_names = list(df.columns)
            placeholders = ", ".join(["?" for _ in column_names])
            columns_str = ", ".join(column_names)
            insert_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
            
            # Insert each row
            prepared = self.session.prepare(insert_query)
            for _, row in df.iterrows():
                values = [row[col] for col in column_names]
                self.session.execute(prepared, values)
                
            logger.info(f"Successfully inserted {len(df)} rows from DataFrame into {table}")
            
        except Exception as e:
            logger.error(f"Failed to insert DataFrame data: {str(e)}")
            raise
    
    def cqlsh(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a raw CQL command and return results.
        
        Args:
            query (str): CQL query to execute.
            
        Returns:
            List[Dict[str, Any]]: Query results as list of dictionaries.
        """
        self._check_cassandra_availability()
        
        if not self.session:
            self.connect()
            
        try:
            result = self.session.execute(query)
            # Convert result to list of dictionaries
            rows = []
            for row in result:
                rows.append(row._asdict())
            logger.info(f"Executed CQL query: {query}")
            return rows
        except Exception as e:
            logger.error(f"Failed to execute CQL query: {str(e)}")
            raise
    
    def close(self) -> None:
        """
        Close the Cassandra connection.
        """
        if not CASSANDRA_AVAILABLE or not self.cluster:
            return
            
        try:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")
        except Exception as e:
            logger.error(f"Error closing Cassandra connection: {str(e)}")
