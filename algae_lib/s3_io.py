"""
S3 I/O operations for BarAlgae data infrastructure.

This module provides utilities for interacting with S3-compatible storage
(MinIO for local development, AWS S3 for production).
"""

import os
import logging
from typing import Optional, List
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)


class S3Manager:
    """Manages S3 operations for the BarAlgae data infrastructure."""
    
    def __init__(self):
        """Initialize S3 client with environment configuration."""
        self.endpoint_url = os.getenv("S3_ENDPOINT_URL")
        self.access_key = os.getenv("S3_ACCESS_KEY")
        self.secret_key = os.getenv("S3_SECRET_KEY")
        self.region = os.getenv("S3_REGION", "us-east-1")
        self.bucket = os.getenv("S3_BUCKET", "baralgae-bronze")
        
        self._client = None
        self._resource = None
    
    @property
    def client(self):
        """Get S3 client, creating it if necessary."""
        if self._client is None:
            try:
                self._client = boto3.client(
                    's3',
                    endpoint_url=self.endpoint_url,
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key,
                    region_name=self.region
                )
            except NoCredentialsError:
                logger.error("S3 credentials not found")
                raise
        return self._client
    
    @property
    def resource(self):
        """Get S3 resource, creating it if necessary."""
        if self._resource is None:
            try:
                self._resource = boto3.resource(
                    's3',
                    endpoint_url=self.endpoint_url,
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key,
                    region_name=self.region
                )
            except NoCredentialsError:
                logger.error("S3 credentials not found")
                raise
        return self._resource
    
    def ensure_bucket(self, bucket_name: Optional[str] = None) -> bool:
        """
        Ensure the specified bucket exists, creating it if necessary.
        
        Args:
            bucket_name: Name of the bucket. Uses default if None.
            
        Returns:
            True if bucket exists or was created successfully.
        """
        bucket_name = bucket_name or self.bucket
        
        try:
            self.client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket {bucket_name} already exists")
            return True
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                # Bucket doesn't exist, create it
                try:
                    self.client.create_bucket(Bucket=bucket_name)
                    logger.info(f"Created bucket {bucket_name}")
                    return True
                except ClientError as create_error:
                    logger.error(f"Failed to create bucket {bucket_name}: {create_error}")
                    return False
            else:
                logger.error(f"Error checking bucket {bucket_name}: {e}")
                return False
    
    def upload_file(self, file_path: str, s3_key: str, bucket_name: Optional[str] = None) -> bool:
        """
        Upload a file to S3.
        
        Args:
            file_path: Local path to the file to upload.
            s3_key: S3 key (path) where the file should be stored.
            bucket_name: Name of the bucket. Uses default if None.
            
        Returns:
            True if upload was successful.
        """
        bucket_name = bucket_name or self.bucket
        
        if not self.ensure_bucket(bucket_name):
            return False
        
        try:
            self.client.upload_file(file_path, bucket_name, s3_key)
            logger.info(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {file_path}: {e}")
            return False
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            return False
    
    def download_file(self, s3_key: str, local_path: str, bucket_name: Optional[str] = None) -> bool:
        """
        Download a file from S3.
        
        Args:
            s3_key: S3 key (path) of the file to download.
            local_path: Local path where the file should be saved.
            bucket_name: Name of the bucket. Uses default if None.
            
        Returns:
            True if download was successful.
        """
        bucket_name = bucket_name or self.bucket
        
        try:
            # Ensure local directory exists
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            
            self.client.download_file(bucket_name, s3_key, local_path)
            logger.info(f"Downloaded s3://{bucket_name}/{s3_key} to {local_path}")
            return True
        except ClientError as e:
            logger.error(f"Failed to download s3://{bucket_name}/{s3_key}: {e}")
            return False
    
    def list_objects(self, prefix: str = "", bucket_name: Optional[str] = None) -> List[str]:
        """
        List objects in S3 bucket with given prefix.
        
        Args:
            prefix: S3 key prefix to filter objects.
            bucket_name: Name of the bucket. Uses default if None.
            
        Returns:
            List of S3 keys matching the prefix.
        """
        bucket_name = bucket_name or self.bucket
        
        try:
            response = self.client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                return []
        except ClientError as e:
            logger.error(f"Failed to list objects in {bucket_name}: {e}")
            return []
    
    def delete_object(self, s3_key: str, bucket_name: Optional[str] = None) -> bool:
        """
        Delete an object from S3.
        
        Args:
            s3_key: S3 key (path) of the object to delete.
            bucket_name: Name of the bucket. Uses default if None.
            
        Returns:
            True if deletion was successful.
        """
        bucket_name = bucket_name or self.bucket
        
        try:
            self.client.delete_object(Bucket=bucket_name, Key=s3_key)
            logger.info(f"Deleted s3://{bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete s3://{bucket_name}/{s3_key}: {e}")
            return False
