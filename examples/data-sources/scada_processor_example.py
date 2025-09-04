#!/usr/bin/env python3
# SECURITY NOTE: synthetic/demo only. No production endpoints, secrets, or schemas.

"""
SCADA Data Processing Example - Based on Production Code
=======================================================

This is a cleaned and sanitized example of the SCADA data processing logic
from the production BarAlgae system. All sensitive information has been removed
and replaced with environment variables for demonstration purposes.

Original functionality:
- Connects to SQL Server database (generic)
- Extracts SCADA sensor data for specified date ranges
- Performs data transformation and unpivoting
- Uploads to S3 with proper partitioning (Bronze layer)
- Handles multiple sensor parameters (CO2, temperature, pH, etc.)

Security Note: This example contains no production secrets or real data.
"""

import pandas as pd
from sqlalchemy import create_engine
import boto3
from datetime import datetime, timedelta
import re
import warnings
import os
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
from typing import Dict, List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SCADAProcessor:
    """
    Processes SCADA sensor data from production systems.
    
    This class handles the complete pipeline from database extraction
    to S3 upload with proper data transformation.
    """
    
    def __init__(self, s3_client, bucket_name: str, db_connection_string: str):
        """
        Initialize SCADA processor.
        
        Args:
            s3_client: Configured S3 client
            bucket_name: Target S3 bucket name
            db_connection_string: Database connection string
        """
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.engine = create_engine(db_connection_string)
        
        # Define column data types for SCADA data
        self.column_types = {
            'DateTime': 'datetime64[ns]',
            'tpu': 'string',
            'reactor': 'string',
            'ParameterValue': 'float',
            'co2valve': 'double',
            'cooltemp': 'double',
            'coolvalve': 'double',
            'fertvalve': 'double',
            'harvvalve': 'double',
            'irigvalve': 'double',
            'level': 'double',
            'pump': 'double',
            'temp': 'double',
            'ph': 'double',
            'LightSensor': 'double',
            'Tank50_Temp': 'double',
            'Tank50_Conductivity': 'double',
            'Tank50_Level': 'double',
            '__index_level_0__': 'bigint',
            'year': 'string',
            'month': 'string',
            'day': 'string'
        }
    
    def extract_scada_data(self, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
        """
        Extract SCADA data from database for specified date range.
        
        Args:
            start_date: Start date for data extraction
            end_date: End date for data extraction
            
        Returns:
            DataFrame with SCADA data or None if error
        """
        try:
            # Formulate the query with date conditions
            scada_table = os.getenv("SCADA_TABLE", "scada_data")
            query = f'''
            SELECT * 
            FROM {scada_table} 
            WHERE DateTime >= '{start_date.date()}' AND DateTime < '{end_date.date()}';
            '''
            
            logger.info(f"Extracting SCADA data from {start_date.date()} to {end_date.date()}")
            
            # Suppress warnings and execute query
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                connection = self.engine.raw_connection()
                try:
                    df = pd.read_sql_query(query, connection)
                finally:
                    connection.close()
            
            if df.empty:
                logger.warning("No data found for the specified date range")
                return None
            
            # Set data types
            for column, dtype in self.column_types.items():
                if column in df.columns:
                    df[column] = df[column].astype(dtype)
            
            logger.info(f"Extracted {len(df)} rows of SCADA data")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting SCADA data: {str(e)}")
            return None
    
    def transform_scada_data(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Transform SCADA data by unpivoting sensor parameters.
        
        Args:
            df: Raw SCADA DataFrame
            
        Returns:
            Transformed DataFrame or None if error
        """
        try:
            if df.empty:
                return None
            
            # Define columns to unpivot (exclude special columns)
            columns_to_unpivot = [col for col in df.columns 
                                if col not in ['LightSensor', 'Tank50_Temp', 
                                             'Tank50_Conductivity', 'Tank50_Level', 
                                             'DateTime']]
            
            # Perform unpivot operation
            df_unpivoted = df.melt(
                id_vars=['DateTime', 'LightSensor', 'Tank50_Temp', 
                        'Tank50_Conductivity', 'Tank50_Level'], 
                value_vars=columns_to_unpivot, 
                var_name='parameter_name', 
                value_name='ParameterValue'
            )
            
            # Add date partitioning columns
            df_unpivoted['year'] = df_unpivoted['DateTime'].dt.year.astype(str)
            df_unpivoted['month'] = df_unpivoted['DateTime'].dt.month.astype(str).str.zfill(2)
            df_unpivoted['day'] = df_unpivoted['DateTime'].dt.day.astype(str).str.zfill(2)
            
            # Add TPU and reactor information (extracted from parameter names)
            df_unpivoted['tpu'] = df_unpivoted['parameter_name'].str.extract(r'tpu_?(\d+)', expand=False)
            df_unpivoted['reactor'] = df_unpivoted['parameter_name'].str.extract(r'reactor_?(\d+)', expand=False)
            
            # Fill missing TPU/reactor with default values
            df_unpivoted['tpu'] = df_unpivoted['tpu'].fillna('1')
            df_unpivoted['reactor'] = df_unpivoted['reactor'].fillna('1')
            
            logger.info(f"Transformed data: {len(df_unpivoted)} rows")
            return df_unpivoted
            
        except Exception as e:
            logger.error(f"Error transforming SCADA data: {str(e)}")
            return None
    
    def upload_to_s3(self, df: pd.DataFrame, year: str, month: str, day: str) -> Optional[str]:
        """
        Upload transformed SCADA data to S3.
        
        Args:
            df: Transformed DataFrame
            year, month, day: Date components for partitioning
            
        Returns:
            S3 key if successful, None otherwise
        """
        try:
            # Convert to Parquet
            table = pa.Table.from_pandas(df)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Generate S3 key with proper partitioning
            s3_key = f"bronze/scada/year={year}/month={month}/day={day}/scada_data.parquet"
            
            # Upload to S3
            self.s3_client.upload_fileobj(
                buffer, 
                self.bucket_name, 
                s3_key
            )
            
            logger.info(f"Successfully uploaded SCADA data to s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Error uploading to S3: {str(e)}")
            return None
    
    def process_date_range(self, start_date: datetime, end_date: datetime) -> Optional[str]:
        """
        Process SCADA data for a specific date range.
        
        Args:
            start_date: Start date for processing
            end_date: End date for processing
            
        Returns:
            S3 key if successful, None otherwise
        """
        try:
            # Extract data
            df = self.extract_scada_data(start_date, end_date)
            if df is None or df.empty:
                return None
            
            # Transform data
            df_transformed = self.transform_scada_data(df)
            if df_transformed is None or df_transformed.empty:
                return None
            
            # Upload to S3
            year, month, day = start_date.strftime("%Y-%m-%d").split("-")
            s3_key = self.upload_to_s3(df_transformed, year, month, day)
            
            return s3_key
            
        except Exception as e:
            logger.error(f"Error processing SCADA data: {str(e)}")
            return None
    
    def process_multiple_days(self, num_days: int = 2) -> List[str]:
        """
        Process SCADA data for multiple days.
        
        Args:
            num_days: Number of days to process
            
        Returns:
            List of uploaded S3 keys
        """
        uploaded_keys = []
        
        for i in range(num_days):
            # Calculate date range for current iteration
            start_date = datetime.now() - timedelta(days=i)
            end_date = datetime.now() - timedelta(days=i-1)
            
            logger.info(f"Processing day {i+1}/{num_days}: {start_date.date()}")
            
            s3_key = self.process_date_range(start_date, end_date)
            if s3_key:
                uploaded_keys.append(s3_key)
        
        logger.info(f"Successfully processed {len(uploaded_keys)} days")
        return uploaded_keys


def main():
    """
    Example usage of SCADA processor.
    
    This demonstrates how the processor would be used in production,
    using environment variables for configuration.
    """
    # S3 configuration using environment variables
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),  # Optional for MinIO
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )
    bucket_name = os.getenv("S3_BUCKET", "baralgae-demo")
    
    # Database connection string using environment variables
    db_connection_string = os.getenv(
        "DATABASE_CONNECTION_STRING",
        "mssql+pyodbc:///?odbc_connect=DRIVER={SQL Server};SERVER=localhost;DATABASE=demo_db;Trusted_Connection=yes;"
    )
    
    # Initialize processor
    processor = SCADAProcessor(s3_client, bucket_name, db_connection_string)
    
    # Process multiple days
    uploaded_keys = processor.process_multiple_days(num_days=2)
    
    print(f"Processed {len(uploaded_keys)} days of SCADA data")
    for key in uploaded_keys:
        print(f"  - {key}")


if __name__ == "__main__":
    main()
