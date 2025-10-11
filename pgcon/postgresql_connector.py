import pandas as pd
from typing import List, Dict, Any, Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import PostgreSQL dependencies, but handle gracefully if not available
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    POSTGRES_AVAILABLE = True
except ImportError as e:
    logger.warning(f"PostgreSQL dependencies not available: {e}")
    POSTGRES_AVAILABLE = False
    psycopg2 = None
    RealDictCursor = None

class Pgcon:
    """
    Pgcon (PostgreSQL Connector) - A module to handle PostgreSQL database connections
    and data operations. For now, handles CSV data processing as a starting point.
    """
    
    def __init__(self, host: str = "127.0.0.1", port: int = 5432, 
                 database: str = "postgres", username: str = "postgres", password: str = "postgres"):
        """
        Initialize the Pgcon instance with PostgreSQL connection parameters.
        
        Args:
            host (str): Host address of the PostgreSQL server. Defaults to "127.0.0.1".
            port (int): Port number for PostgreSQL connection. Defaults to 5432.
            database (str): Name of the database to connect to. Defaults to "postgres".
            username (str): Username for authentication. Defaults to "postgres".
            password (str): Password for authentication. Defaults to "postgres".
        """
        if not POSTGRES_AVAILABLE:
            logger.warning("PostgreSQL dependencies not installed. "
                          "Only CSV processing functionality will be available.")
        
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.connection = None
        self.data = None
        
        if not POSTGRES_AVAILABLE:
            logger.info("Running in CSV-only mode. Install psycopg2 for full functionality.")
    
    def _check_postgres_availability(self):
        """Check if PostgreSQL dependencies are available."""
        if not POSTGRES_AVAILABLE:
            raise ImportError("PostgreSQL dependencies not available. "
                             "Please install psycopg2 package for full functionality. "
                             "Current version supports CSV processing only.")
    
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
        Establish connection to the PostgreSQL database.
        """
        self._check_postgres_availability()
        
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password
            )
            logger.info(f"Connected to PostgreSQL database at {self.host}:{self.port}/{self.database}")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results.
        
        Args:
            query (str): SQL query to execute.
            params (Optional[tuple]): Parameters for the query.
            
        Returns:
            List[Dict[str, Any]]: Query results as list of dictionaries.
        """
        self._check_postgres_availability()
        
        if not self.connection:
            self.connect()
            
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query, params)
            
            # If it's a SELECT query, fetch results
            if query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                # Convert to list of dictionaries
                rows = [dict(row) for row in results]
                logger.info(f"Executed query and fetched {len(rows)} rows")
                return rows
            else:
                # For INSERT, UPDATE, DELETE, etc., commit and return affected rows
                self.connection.commit()
                affected_rows = cursor.rowcount
                logger.info(f"Executed query affecting {affected_rows} rows")
                return [{"affected_rows": affected_rows}]
                
        except Exception as e:
            logger.error(f"Failed to execute query: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def create_table_from_csv(self, file_path: str, table_name: str, 
                             columns: Optional[List[str]] = None) -> None:
        """
        Create a table based on CSV structure and insert data.
        
        Args:
            file_path (str): Path to the CSV file.
            table_name (str): Name of the table to create.
            columns (Optional[List[str]]): List of specific columns to load. 
                                          If None, loads all columns.
        """
        self._check_postgres_availability()
        
        if not self.connection:
            self.connect()
            
        try:
            # Load CSV data to infer schema
            if columns:
                df = pd.read_csv(file_path, usecols=columns)
            else:
                df = pd.read_csv(file_path)
            
            # Infer PostgreSQL column types from pandas dtypes
            column_definitions = []
            for col in df.columns:
                dtype = df[col].dtype
                if pd.api.types.is_integer_dtype(dtype):
                    pg_type = "INTEGER"
                elif pd.api.types.is_float_dtype(dtype):
                    pg_type = "DOUBLE PRECISION"
                else:
                    # For object/string types, estimate max length
                    max_length = df[col].astype(str).str.len().max()
                    if max_length < 255:
                        pg_type = f"VARCHAR({max_length + 10})"
                    else:
                        pg_type = "TEXT"
                
                column_definitions.append(f"{col} {pg_type}")
            
            # Create table
            columns_def = ", ".join(column_definitions)
            create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})"
            self.execute_query(create_query)
            logger.info(f"Created table {table_name}")
            
            # Insert data using pandas to_sql method
            df.to_sql(table_name, self.connection, if_exists='append', index=False)
            logger.info(f"Inserted {len(df)} rows into {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to create table from CSV: {str(e)}")
            raise
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Insert data from a pandas DataFrame into a PostgreSQL table.
        
        Args:
            df (pd.DataFrame): DataFrame containing the data to insert.
            table_name (str): Name of the target table.
        """
        self._check_postgres_availability()
        
        if not self.connection:
            self.connect()
            
        try:
            # Insert data using pandas to_sql method
            df.to_sql(table_name, self.connection, if_exists='append', index=False)
            logger.info(f"Successfully inserted {len(df)} rows from DataFrame into {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to insert DataFrame data: {str(e)}")
            raise
    
    def close(self) -> None:
        """
        Close the PostgreSQL connection.
        """
        if not POSTGRES_AVAILABLE or not self.connection:
            return
            
        try:
            self.connection.close()
            logger.info("PostgreSQL connection closed")
        except Exception as e:
            logger.error(f"Error closing PostgreSQL connection: {str(e)}")