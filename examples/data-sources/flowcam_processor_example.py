#!/usr/bin/env python3
# SECURITY NOTE: synthetic/demo only. No production endpoints, secrets, or schemas.

"""
FlowCam Data Processing Example - Based on Production Code
==========================================================

This is a cleaned and sanitized example of the FlowCam data processing logic
from the production BarAlgae system. All sensitive information has been removed
and replaced with environment variables for demonstration purposes.

Original functionality:
- Processes FlowCam microscopy data files (.lst format)
- Uploads to S3 with proper partitioning (Bronze layer)
- Handles multiple algae types (pt, nano, algae health, TS, TW)
- Extracts TPU and reactor information from folder structure
- Converts to Parquet format for efficient storage

Security Note: This example contains no production secrets or real data.
"""

import os
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
import re
import tempfile
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Dict, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FlowCamProcessor:
    """
    Processes FlowCam microscopy data for algae analysis.
    
    This class handles the complete pipeline from file discovery
    to S3 upload with proper data partitioning.
    """
    
    def __init__(self, s3_client, bucket_name: str):
        """
        Initialize FlowCam processor.
        
        Args:
            s3_client: Configured S3 client
            bucket_name: Target S3 bucket name
        """
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.algae_types = ["pt", "nano", "algae health", "TS", "TW"]
        
    def get_date_range(self, days_back: int = 3) -> tuple:
        """
        Calculate date range for processing.
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            Tuple of (start_date, end_date)
        """
        today = datetime.now()
        start_date = today - timedelta(days=days_back)
        return start_date, today
    
    def extract_tpu_reactor_info(self, folder_name: str) -> tuple:
        """
        Extract TPU and reactor information from folder name.
        
        Args:
            folder_name: Folder name containing TPU.reactor format
            
        Returns:
            Tuple of (tpu_id, reactor_id)
        """
        # Extract TPU ID (left of first dot)
        tpu_match = re.search(r"(\d+)\.", folder_name)
        tpu_id = tpu_match.group(1) if tpu_match else "unknown"
        
        # Extract reactor ID (two digits after first dot)
        reactor_match = re.search(r"\.(\d{2})", folder_name)
        reactor_id = reactor_match.group(1) if reactor_match else "unknown"
        
        return tpu_id, reactor_id
    
    def process_flowcam_file(self, file_path: str, algae_type: str, 
                           tpu_id: str, reactor_id: str, 
                           year: str, month: str, day: str) -> Optional[str]:
        """
        Process a single FlowCam file and upload to S3.
        
        Args:
            file_path: Path to the .lst file
            algae_type: Type of algae (pt, nano, etc.)
            tpu_id: TPU identifier
            reactor_id: Reactor identifier
            year, month, day: Date components
            
        Returns:
            S3 key if successful, None otherwise
        """
        try:
            # Read the .lst file
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Basic validation
            if not content.strip():
                logger.warning(f"Empty file: {file_path}")
                return None
            
            # Create DataFrame from content
            lines = content.strip().split('\n')
            data = []
            
            for line in lines:
                if line.strip() and not line.startswith('#'):
                    # Parse FlowCam data line (simplified)
                    parts = line.strip().split()
                    if len(parts) >= 3:  # Minimum required fields
                        data.append({
                            'timestamp': parts[0] if len(parts) > 0 else '',
                            'particle_id': parts[1] if len(parts) > 1 else '',
                            'area': float(parts[2]) if len(parts) > 2 and parts[2].replace('.', '').isdigit() else 0.0,
                            'algae_type': algae_type,
                            'tpu_id': tpu_id,
                            'reactor_id': reactor_id,
                            'processing_date': datetime.now().isoformat()
                        })
            
            if not data:
                logger.warning(f"No valid data in file: {file_path}")
                return None
            
            # Create DataFrame
            df = pd.DataFrame(data)
            
            # Convert to Parquet
            table = pa.Table.from_pandas(df)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Generate S3 key with proper partitioning
            s3_key = (f"bronze/flowcam/algae={algae_type.replace(' ', '_')}/"
                     f"year={year}/month={month}/day={day}/"
                     f"tpu={tpu_id}/reactor={reactor_id}/"
                     f"flowcam_data.parquet")
            
            # Upload to S3
            self.s3_client.upload_fileobj(
                buffer, 
                self.bucket_name, 
                s3_key
            )
            
            logger.info(f"Successfully uploaded {file_path} to s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            return None
    
    def process_directory(self, base_directory: str, days_back: int = 3) -> List[str]:
        """
        Process all FlowCam files in directory structure.
        
        Args:
            base_directory: Base directory containing algae type folders
            days_back: Number of days to look back
            
        Returns:
            List of uploaded S3 keys
        """
        uploaded_keys = []
        start_date, end_date = self.get_date_range(days_back)
        
        logger.info(f"Processing files from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        if not os.path.exists(base_directory):
            logger.error(f"Directory not found: {base_directory}")
            return uploaded_keys
        
        # Process each algae type folder
        for algae_type in self.algae_types:
            folder_path = os.path.join(base_directory, algae_type)
            
            if not os.path.exists(folder_path):
                logger.warning(f"Algae type folder not found: {folder_path}")
                continue
            
            logger.info(f"Processing algae type: {algae_type}")
            
            # Get subfolders (TPU.reactor format)
            try:
                subfolders = [f for f in os.listdir(folder_path) 
                            if os.path.isdir(os.path.join(folder_path, f))]
            except Exception as e:
                logger.error(f"Error reading folder {folder_path}: {e}")
                continue
            
            for subfolder in subfolders:
                subfolder_path = os.path.join(folder_path, subfolder)
                
                try:
                    # Check if folder was modified within date range
                    mod_time = datetime.fromtimestamp(os.path.getmtime(subfolder_path))
                    if not (start_date <= mod_time <= end_date):
                        continue
                    
                    # Extract TPU and reactor info
                    tpu_id, reactor_id = self.extract_tpu_reactor_info(subfolder)
                    
                    # Process files in subfolder
                    try:
                        files = [f for f in os.listdir(subfolder_path) 
                                if os.path.isfile(os.path.join(subfolder_path, f))]
                    except Exception as e:
                        logger.error(f"Error reading files in {subfolder_path}: {e}")
                        continue
                    
                    for file in files:
                        if file.lower().endswith('.lst'):
                            file_path = os.path.join(subfolder_path, file)
                            
                            # Extract date components
                            year, month, day = mod_time.strftime("%Y-%m-%d").split("-")
                            
                            # Process file
                            s3_key = self.process_flowcam_file(
                                file_path, algae_type, tpu_id, reactor_id,
                                year, month, day
                            )
                            
                            if s3_key:
                                uploaded_keys.append(s3_key)
                
                except Exception as e:
                    logger.error(f"Error processing subfolder {subfolder}: {e}")
                    continue
        
        logger.info(f"Successfully processed {len(uploaded_keys)} files")
        return uploaded_keys


def main():
    """
    Example usage of FlowCam processor.
    
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
    processor = FlowCamProcessor(s3_client, bucket_name)
    
    # Process files (example directory)
    base_directory = "/opt/airflow/volumes"  # Example path
    uploaded_keys = processor.process_directory(base_directory, days_back=3)
    
    print(f"Processed {len(uploaded_keys)} files")
    for key in uploaded_keys:
        print(f"  - {key}")


if __name__ == "__main__":
    main()
