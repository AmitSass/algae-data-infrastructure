"""
Unit tests for S3 I/O operations.

This module contains unit tests for the S3Manager class
in the algae_lib.s3_io module.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import os
from algae_lib.s3_io import S3Manager


class TestS3Manager:
    """Test class for S3Manager."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        with patch.dict(os.environ, {
            'S3_ENDPOINT_URL': 'http://localhost:9000',
            'S3_ACCESS_KEY': 'test_key',
            'S3_SECRET_KEY': 'test_secret',
            'S3_REGION': 'us-east-1',
            'S3_BUCKET': 'test-bucket'
        }):
            self.s3_manager = S3Manager()
    
    @patch('algae_lib.s3_io.boto3.client')
    def test_client_creation(self, mock_boto3_client):
        """Test S3 client creation."""
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client
        
        client = self.s3_manager.client
        
        assert client == mock_client
        mock_boto3_client.assert_called_once_with(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='test_key',
            aws_secret_access_key='test_secret',
            region_name='us-east-1'
        )
    
    @patch('algae_lib.s3_io.boto3.resource')
    def test_resource_creation(self, mock_boto3_resource):
        """Test S3 resource creation."""
        mock_resource = Mock()
        mock_boto3_resource.return_value = mock_resource
        
        resource = self.s3_manager.resource
        
        assert resource == mock_resource
        mock_boto3_resource.assert_called_once_with(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='test_key',
            aws_secret_access_key='test_secret',
            region_name='us-east-1'
        )
    
    @patch('algae_lib.s3_io.boto3.client')
    def test_ensure_bucket_exists(self, mock_boto3_client):
        """Test ensure_bucket when bucket already exists."""
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client
        mock_client.head_bucket.return_value = {}
        
        result = self.s3_manager.ensure_bucket('test-bucket')
        
        assert result is True
        mock_client.head_bucket.assert_called_once_with(Bucket='test-bucket')
    
    @patch('algae_lib.s3_io.boto3.client')
    def test_ensure_bucket_create(self, mock_boto3_client):
        """Test ensure_bucket when bucket needs to be created."""
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client
        
        # Mock head_bucket to raise 404 error (bucket doesn't exist)
        from botocore.exceptions import ClientError
        error_response = {'Error': {'Code': '404'}}
        mock_client.head_bucket.side_effect = ClientError(error_response, 'HeadBucket')
        
        result = self.s3_manager.ensure_bucket('test-bucket')
        
        assert result is True
        mock_client.head_bucket.assert_called_once_with(Bucket='test-bucket')
        mock_client.create_bucket.assert_called_once_with(Bucket='test-bucket')
    
    @patch('algae_lib.s3_io.boto3.client')
    def test_upload_file_success(self, mock_boto3_client):
        """Test successful file upload."""
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client
        mock_client.head_bucket.return_value = {}
        
        with patch('os.path.exists', return_value=True):
            result = self.s3_manager.upload_file('test.csv', 'test-key')
        
        assert result is True
        mock_client.upload_file.assert_called_once_with('test.csv', 'test-bucket', 'test-key')
    
    @patch('algae_lib.s3_io.boto3.client')
    def test_upload_file_not_found(self, mock_boto3_client):
        """Test file upload when file doesn't exist."""
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client
        mock_client.head_bucket.return_value = {}
        
        with patch('os.path.exists', return_value=False):
            result = self.s3_manager.upload_file('nonexistent.csv', 'test-key')
        
        assert result is False
        mock_client.upload_file.assert_not_called()
    
    @patch('algae_lib.s3_io.boto3.client')
    def test_download_file_success(self, mock_boto3_client):
        """Test successful file download."""
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client
        
        with patch('pathlib.Path.mkdir'):
            result = self.s3_manager.download_file('test-key', 'local-file.csv')
        
        assert result is True
        mock_client.download_file.assert_called_once_with('test-bucket', 'test-key', 'local-file.csv')
    
    @patch('algae_lib.s3_io.boto3.client')
    def test_list_objects_success(self, mock_boto3_client):
        """Test successful object listing."""
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client
        mock_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'test1.csv'},
                {'Key': 'test2.csv'}
            ]
        }
        
        result = self.s3_manager.list_objects('test-prefix')
        
        assert result == ['test1.csv', 'test2.csv']
        mock_client.list_objects_v2.assert_called_once_with(
            Bucket='test-bucket',
            Prefix='test-prefix'
        )
    
    @patch('algae_lib.s3_io.boto3.client')
    def test_list_objects_empty(self, mock_boto3_client):
        """Test object listing when no objects exist."""
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client
        mock_client.list_objects_v2.return_value = {}
        
        result = self.s3_manager.list_objects('test-prefix')
        
        assert result == []
    
    @patch('algae_lib.s3_io.boto3.client')
    def test_delete_object_success(self, mock_boto3_client):
        """Test successful object deletion."""
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client
        
        result = self.s3_manager.delete_object('test-key')
        
        assert result is True
        mock_client.delete_object.assert_called_once_with(Bucket='test-bucket', Key='test-key')
