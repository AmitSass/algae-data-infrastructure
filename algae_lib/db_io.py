"""
Database I/O operations for BarAlgae data infrastructure.

This module provides utilities for interacting with PostgreSQL databases
for both local development and production environments.
"""

import os
import logging
from typing import Optional, List, Dict, Any
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database operations for the BarAlgae data infrastructure."""
    
    def __init__(self):
        """Initialize database connection with environment configuration."""
        self.host = os.getenv("PGHOST", "localhost")
        self.port = os.getenv("PGPORT", "5432")
        self.database = os.getenv("PGDATABASE", "warehouse")
        self.user = os.getenv("PGUSER", "dbuser")
        self.password = os.getenv("PGPASSWORD", "dbpass")
        
        self._engine = None
        self._connection = None
    
    @property
    def connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    @property
    def engine(self):
        """Get SQLAlchemy engine, creating it if necessary."""
        if self._engine is None:
            try:
                self._engine = create_engine(
                    self.connection_string,
                    pool_pre_ping=True,
                    pool_recycle=300
                )
                logger.info(f"Created database engine for {self.host}:{self.port}/{self.database}")
            except SQLAlchemyError as e:
                logger.error(f"Failed to create database engine: {e}")
                raise
        return self._engine
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection is successful.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results.
        
        Args:
            query: SQL query to execute.
            params: Query parameters.
            
        Returns:
            List of dictionaries containing query results.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return [dict(row._mapping) for row in result]
        except SQLAlchemyError as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def execute_command(self, command: str, params: Optional[Dict[str, Any]] = None) -> bool:
        """
        Execute a DDL/DML command (CREATE, INSERT, UPDATE, DELETE).
        
        Args:
            command: SQL command to execute.
            params: Command parameters.
            
        Returns:
            True if command was successful.
        """
        try:
            with self.engine.connect() as conn:
                with conn.begin():
                    conn.execute(text(command), params or {})
            logger.info("Command executed successfully")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Command execution failed: {e}")
            return False
    
    def load_csv_to_table(self, csv_path: str, table_name: str, 
                         if_exists: str = "replace", 
                         index: bool = False,
                         chunksize: int = 1000) -> bool:
        """
        Load CSV data into a database table.
        
        Args:
            csv_path: Path to the CSV file.
            table_name: Name of the target table.
            if_exists: How to behave if table exists ('replace', 'append', 'fail').
            index: Whether to write DataFrame index as a column.
            chunksize: Number of rows to write at a time.
            
        Returns:
            True if load was successful.
        """
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            logger.info(f"Read CSV file {csv_path} with {len(df)} rows")
            
            # Load to database
            df.to_sql(
                table_name,
                self.engine,
                if_exists=if_exists,
                index=index,
                chunksize=chunksize,
                method='multi'
            )
            logger.info(f"Loaded {len(df)} rows into table {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load CSV to table: {e}")
            return False
    
    def load_dataframe_to_table(self, df: pd.DataFrame, table_name: str,
                               if_exists: str = "replace",
                               index: bool = False,
                               chunksize: int = 1000) -> bool:
        """
        Load DataFrame into a database table.
        
        Args:
            df: DataFrame to load.
            table_name: Name of the target table.
            if_exists: How to behave if table exists ('replace', 'append', 'fail').
            index: Whether to write DataFrame index as a column.
            chunksize: Number of rows to write at a time.
            
        Returns:
            True if load was successful.
        """
        try:
            df.to_sql(
                table_name,
                self.engine,
                if_exists=if_exists,
                index=index,
                chunksize=chunksize,
                method='multi'
            )
            logger.info(f"Loaded {len(df)} rows into table {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load DataFrame to table: {e}")
            return False
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Name of the table to check.
            
        Returns:
            True if table exists.
        """
        try:
            query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = :table_name
            );
            """
            result = self.execute_query(query, {"table_name": table_name})
            return result[0]['exists'] if result else False
        except Exception as e:
            logger.error(f"Failed to check if table exists: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get information about table columns.
        
        Args:
            table_name: Name of the table.
            
        Returns:
            List of dictionaries with column information.
        """
        try:
            query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = :table_name
            ORDER BY ordinal_position;
            """
            return self.execute_query(query, {"table_name": table_name})
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            return []
    
    def close_connections(self):
        """Close all database connections."""
        if self._engine:
            self._engine.dispose()
            logger.info("Database connections closed")
