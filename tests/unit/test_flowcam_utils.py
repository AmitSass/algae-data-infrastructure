"""
Unit tests for FlowCam utilities.

This module contains unit tests for the FlowCamProcessor class
in the algae_lib.flowcam_utils module.
"""

import pytest
import pandas as pd
from datetime import datetime
from algae_lib.flowcam_utils import FlowCamProcessor


class TestFlowCamProcessor:
    """Test class for FlowCamProcessor."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.processor = FlowCamProcessor()
    
    def test_read_flowcam_csv_success(self):
        """Test successful CSV reading."""
        # Create test data
        test_data = {
            'date': ['2024-01-01', '2024-01-02'],
            'tpu': [1, 2],
            'reactor': [1, 2],
            'algae_density': [1.2, 1.5]
        }
        df = pd.DataFrame(test_data)
        
        with pytest.MonkeyPatch().context() as m:
            m.setattr(pd, 'read_csv', lambda x: df)
            result = self.processor.read_flowcam_csv('test.csv')
        
        assert result is not None
        assert len(result) == 2
        assert list(result.columns) == ['date', 'tpu', 'reactor', 'algae_density']
    
    def test_read_flowcam_csv_failure(self):
        """Test CSV reading failure."""
        with pytest.MonkeyPatch().context() as m:
            m.setattr(pd, 'read_csv', side_effect=Exception("File not found"))
            result = self.processor.read_flowcam_csv('nonexistent.csv')
        
        assert result is None
    
    def test_validate_flowcam_data_valid(self):
        """Test validation of valid data."""
        test_data = {
            'date': ['2024-01-01', '2024-01-02'],
            'tpu': [1, 2],
            'reactor': [1, 2],
            'algae_density': [1.2, 1.5]
        }
        df = pd.DataFrame(test_data)
        
        result = self.processor.validate_flowcam_data(df)
        
        assert result['is_valid'] is True
        assert len(result['errors']) == 0
        assert result['row_count'] == 2
    
    def test_validate_flowcam_data_missing_columns(self):
        """Test validation with missing columns."""
        test_data = {
            'date': ['2024-01-01'],
            'tpu': [1]
            # Missing 'reactor' and 'algae_density' columns
        }
        df = pd.DataFrame(test_data)
        
        result = self.processor.validate_flowcam_data(df)
        
        assert result['is_valid'] is False
        assert len(result['errors']) > 0
        assert 'Missing required columns' in result['errors'][0]
    
    def test_validate_flowcam_data_invalid_values(self):
        """Test validation with invalid values."""
        test_data = {
            'date': ['2024-01-01', 'invalid-date'],
            'tpu': [1, 15],  # Out of range
            'reactor': [1, 25],  # Out of range
            'algae_density': [1.2, 5.0]  # Out of range
        }
        df = pd.DataFrame(test_data)
        
        result = self.processor.validate_flowcam_data(df)
        
        assert result['is_valid'] is True  # Should still be valid
        assert len(result['warnings']) > 0  # But with warnings
    
    def test_clean_flowcam_data(self):
        """Test data cleaning."""
        test_data = {
            'date': ['2024-01-01', 'invalid-date', '2024-01-03'],
            'tpu': [1, 2, 3],
            'reactor': [1, 2, 3],
            'algae_density': [1.2, 5.0, 1.5]  # One out of range
        }
        df = pd.DataFrame(test_data)
        
        result = self.processor.clean_flowcam_data(df)
        
        # Should remove invalid entries
        assert len(result) < len(df)
        assert result['algae_density'].max() <= 3.0
        assert result['algae_density'].min() >= 0.0
    
    def test_add_derived_columns(self):
        """Test adding derived columns."""
        test_data = {
            'date': pd.to_datetime(['2024-01-01', '2024-01-02']),
            'tpu': [1, 2],
            'reactor': [1, 2],
            'algae_density': [1.2, 1.5]
        }
        df = pd.DataFrame(test_data)
        
        result = self.processor.add_derived_columns(df)
        
        # Check that derived columns were added
        assert 'year' in result.columns
        assert 'month' in result.columns
        assert 'day' in result.columns
        assert 'density_category' in result.columns
        assert 'density_change' in result.columns
    
    def test_calculate_daily_aggregates(self):
        """Test daily aggregation calculation."""
        test_data = {
            'date': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-02']),
            'tpu': [1, 1, 1],
            'reactor': [1, 1, 1],
            'algae_density': [1.2, 1.4, 1.6]
        }
        df = pd.DataFrame(test_data)
        
        result = self.processor.calculate_daily_aggregates(df)
        
        assert len(result) == 2  # Two unique date/tpu/reactor combinations
        assert 'avg_density' in result.columns
        assert 'min_density' in result.columns
        assert 'max_density' in result.columns
        assert 'std_density' in result.columns
        assert 'measurement_count' in result.columns
    
    def test_process_flowcam_file_success(self):
        """Test complete file processing pipeline."""
        test_data = {
            'date': ['2024-01-01', '2024-01-02'],
            'tpu': [1, 2],
            'reactor': [1, 2],
            'algae_density': [1.2, 1.5]
        }
        df = pd.DataFrame(test_data)
        
        with pytest.MonkeyPatch().context() as m:
            m.setattr(pd, 'read_csv', lambda x: df)
            result = self.processor.process_flowcam_file('test.csv')
        
        assert result is not None
        assert len(result) == 2
        assert 'year' in result.columns  # Derived columns should be present
        assert 'density_category' in result.columns
    
    def test_process_flowcam_file_failure(self):
        """Test file processing failure."""
        with pytest.MonkeyPatch().context() as m:
            m.setattr(pd, 'read_csv', side_effect=Exception("File not found"))
            result = self.processor.process_flowcam_file('nonexistent.csv')
        
        assert result is None
