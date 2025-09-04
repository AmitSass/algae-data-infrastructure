#!/usr/bin/env python3
# SECURITY NOTE: synthetic/demo only. No production endpoints, secrets, or schemas.

"""
Harvest Documentation Processing Example - Based on Production Code
=================================================================

This is a cleaned and sanitized example of the harvest documentation processing logic
from the production BarAlgae system. All sensitive information has been removed
and replaced with environment variables for demonstration purposes.

Original functionality:
- Processes harvest documentation Excel files from network volumes
- Performs data cleaning and column renaming
- Handles file permissions and network access
- Uploads to S3 with proper partitioning (Bronze layer)
- Validates data integrity and completeness

Security Note: This example contains no production secrets or real data.
"""

import os
import re
import shutil
import tempfile
from datetime import datetime, timedelta
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from typing import Dict, List, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HarvestProcessor:
    """
    Processes harvest documentation data from network volumes.
    
    This class handles the complete pipeline from file discovery
    to S3 upload with proper data transformation and validation.
    """
    
    def __init__(self, s3_client, bucket_name: str, 
                 volumes_path: str = "/opt/airflow/volumes"):
        """
        Initialize harvest processor.
        
        Args:
            s3_client: Configured S3 client
            bucket_name: Target S3 bucket name
            volumes_path: Base path for network volumes
        """
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.volumes_path = volumes_path
        self.harvest_room_path = os.path.join(volumes_path, "harvest_room")
        
        # Expected file patterns
        self.file_patterns = [
            "harvest_documentation.xlsx",
            "harvest_*.xlsx",
            "harvest_data_*.xlsx"
        ]
        
        # Column mapping for harvest data
        self.column_mapping = {
            'תאריך': 'date',
            'TPU': 'tpu',
            'ראקטור': 'reactor',
            'סוג אצה': 'algae_type',
            'נפח קציר (L)': 'harvest_volume_l',
            'צפיפות קציר': 'harvest_density',
            'איכות': 'quality',
            'מבצע': 'operator',
            'הערות': 'comments',
            'משקל (kg)': 'weight_kg',
            'pH': 'ph',
            'טמפרטורה (°C)': 'temperature_c',
            'מוליכות': 'conductivity',
            'חמצן מומס (mg/L)': 'dissolved_oxygen',
            'חנקן (ppm)': 'nitrogen_ppm',
            'זרחן (ppm)': 'phosphorus_ppm'
        }
    
    def check_file_permissions(self, file_path: str) -> bool:
        """
        Check file permissions and accessibility.
        
        Args:
            file_path: Path to file to check
            
        Returns:
            True if accessible, False otherwise
        """
        try:
            if not os.path.exists(file_path):
                logger.warning(f"File not found: {file_path}")
                return False
            
            # Check file size
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                logger.warning(f"Empty file: {file_path}")
                return False
            
            # Check modification time
            mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            age_days = (datetime.now() - mod_time).days
            
            if age_days > 30:  # Files older than 30 days
                logger.warning(f"Old file (>{age_days} days): {file_path}")
            
            logger.info(f"File accessible: {file_path} (size: {file_size} bytes, age: {age_days} days)")
            return True
            
        except Exception as e:
            logger.error(f"Error checking file permissions: {str(e)}")
            return False
    
    def find_harvest_files(self) -> List[str]:
        """
        Find harvest documentation files in the volumes directory.
        
        Returns:
            List of file paths
        """
        found_files = []
        
        try:
            if not os.path.exists(self.volumes_path):
                logger.error(f"Volumes path not found: {self.volumes_path}")
                return found_files
            
            # Check harvest_room directory
            if os.path.exists(self.harvest_room_path):
                logger.info(f"Checking harvest room: {self.harvest_room_path}")
                
                try:
                    files = os.listdir(self.harvest_room_path)
                    excel_files = [f for f in files if f.lower().endswith('.xlsx')]
                    
                    logger.info(f"Found {len(excel_files)} Excel files in harvest room")
                    
                    for file in excel_files:
                        file_path = os.path.join(self.harvest_room_path, file)
                        if self.check_file_permissions(file_path):
                            found_files.append(file_path)
                            
                except Exception as e:
                    logger.error(f"Error reading harvest room directory: {str(e)}")
            
            # Check other potential locations
            for root, dirs, files in os.walk(self.volumes_path):
                for file in files:
                    if file.lower().endswith('.xlsx') and 'harvest' in file.lower():
                        file_path = os.path.join(root, file)
                        if self.check_file_permissions(file_path):
                            found_files.append(file_path)
            
            logger.info(f"Found {len(found_files)} harvest files total")
            return found_files
            
        except Exception as e:
            logger.error(f"Error finding harvest files: {str(e)}")
            return found_files
    
    def clean_column_name(self, col_name: str) -> str:
        """
        Clean and standardize column name.
        
        Args:
            col_name: Original column name
            
        Returns:
            Cleaned column name
        """
        # Remove extra whitespace
        col_name = str(col_name).strip()
        
        # Apply mapping if available
        if col_name in self.column_mapping:
            col_name = self.column_mapping[col_name]
        
        # Convert to lowercase and replace spaces/special chars
        col_name = col_name.lower()
        col_name = re.sub(r'\s+', '_', col_name)
        col_name = re.sub(r'[^\w]', '_', col_name)
        col_name = re.sub(r'_+', '_', col_name).strip('_')
        
        return col_name
    
    def process_harvest_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Process a single harvest documentation file.
        
        Args:
            file_path: Path to the Excel file
            
        Returns:
            Processed DataFrame or None if error
        """
        try:
            logger.info(f"Processing harvest file: {file_path}")
            
            # Read Excel file
            df = pd.read_excel(file_path)
            
            if df.empty:
                logger.warning(f"Empty file: {file_path}")
                return None
            
            # Clean column names
            df.columns = [self.clean_column_name(col) for col in df.columns]
            
            # Add metadata
            df['source_file'] = os.path.basename(file_path)
            df['processing_date'] = datetime.now().isoformat()
            df['file_modification_time'] = datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
            
            # Data type conversions
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
            
            # Convert numeric columns
            numeric_columns = [
                'harvest_volume_l', 'harvest_density', 'weight_kg', 'ph',
                'temperature_c', 'conductivity', 'dissolved_oxygen',
                'nitrogen_ppm', 'phosphorus_ppm'
            ]
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Add date partitioning columns
            if 'date' in df.columns:
                df['year'] = df['date'].dt.year.astype(str)
                df['month'] = df['date'].dt.month.astype(str).str.zfill(2)
                df['day'] = df['date'].dt.day.astype(str).str.zfill(2)
            else:
                # Use processing date if no date column
                df['year'] = datetime.now().year
                df['month'] = datetime.now().month
                df['day'] = datetime.now().day
            
            # Data validation
            required_columns = ['tpu', 'reactor']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"Missing required columns: {missing_columns}")
            
            logger.info(f"Processed {len(df)} rows from {file_path}")
            return df
            
        except Exception as e:
            logger.error(f"Error processing harvest file {file_path}: {str(e)}")
            return None
    
    def upload_to_s3(self, df: pd.DataFrame, year: str, month: str, day: str) -> Optional[str]:
        """
        Upload harvest data to S3.
        
        Args:
            df: Harvest DataFrame
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
            s3_key = f"bronze/harvest/year={year}/month={month}/day={day}/harvest_documentation.parquet"
            
            # Upload to S3
            self.s3_client.upload_fileobj(
                buffer, 
                self.bucket_name, 
                s3_key
            )
            
            logger.info(f"Successfully uploaded harvest data to s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Error uploading harvest data to S3: {str(e)}")
            return None
    
    def process_all_harvest_files(self) -> List[str]:
        """
        Process all harvest documentation files and upload to S3.
        
        Returns:
            List of uploaded S3 keys
        """
        uploaded_keys = []
        
        # Find harvest files
        harvest_files = self.find_harvest_files()
        if not harvest_files:
            logger.warning("No harvest files found for processing")
            return uploaded_keys
        
        # Process each file
        for file_path in harvest_files:
            try:
                df = self.process_harvest_file(file_path)
                if df is not None and not df.empty:
                    # Upload to S3
                    year = datetime.now().strftime("%Y")
                    month = datetime.now().strftime("%m")
                    day = datetime.now().strftime("%d")
                    
                    s3_key = self.upload_to_s3(df, year, month, day)
                    if s3_key:
                        uploaded_keys.append(s3_key)
                        
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
                continue
        
        logger.info(f"Successfully processed {len(uploaded_keys)} harvest files")
        return uploaded_keys


def main():
    """
    Example usage of harvest processor.
    
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
    
    # Initialize processor
    processor = HarvestProcessor(
        s3_client, 
        bucket_name,
        volumes_path=os.getenv("VOLUMES_PATH", "/opt/airflow/volumes")
    )
    
    # Process all harvest files
    uploaded_keys = processor.process_all_harvest_files()
    
    print(f"Processed {len(uploaded_keys)} harvest files")
    for key in uploaded_keys:
        print(f"  - {key}")


if __name__ == "__main__":
    main()
