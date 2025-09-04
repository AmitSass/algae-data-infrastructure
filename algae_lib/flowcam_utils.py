"""
FlowCam data processing utilities for BarAlgae data infrastructure.

This module provides utilities for processing FlowCam microscopy data,
including data validation, type conversions, and basic transformations.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class FlowCamProcessor:
    """Processes FlowCam microscopy data for algae analysis."""
    
    def __init__(self):
        """Initialize FlowCam processor."""
        self.required_columns = ['date', 'tpu', 'reactor', 'algae_density']
        self.valid_tpu_range = (1, 10)  # TPU IDs 1-10
        self.valid_reactor_range = (1, 20)  # Reactor IDs 1-20
        self.valid_density_range = (0.0, 3.0)  # Algae density range
    
    def read_flowcam_csv(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Read FlowCam data from CSV file.
        
        Args:
            file_path: Path to the CSV file.
            
        Returns:
            DataFrame with FlowCam data or None if error.
        """
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Read FlowCam CSV with {len(df)} rows from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Failed to read FlowCam CSV {file_path}: {e}")
            return None
    
    def validate_flowcam_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate FlowCam data structure and content.
        
        Args:
            df: DataFrame to validate.
            
        Returns:
            Dictionary with validation results.
        """
        validation_results = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'row_count': len(df),
            'column_count': len(df.columns)
        }
        
        # Check required columns
        missing_columns = set(self.required_columns) - set(df.columns)
        if missing_columns:
            validation_results['is_valid'] = False
            validation_results['errors'].append(f"Missing required columns: {missing_columns}")
        
        if not validation_results['is_valid']:
            return validation_results
        
        # Validate data types and ranges
        try:
            # Validate date column
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            invalid_dates = df['date'].isna().sum()
            if invalid_dates > 0:
                validation_results['warnings'].append(f"{invalid_dates} invalid dates found")
            
            # Validate TPU column
            df['tpu'] = pd.to_numeric(df['tpu'], errors='coerce')
            invalid_tpu = df['tpu'].isna().sum()
            if invalid_tpu > 0:
                validation_results['warnings'].append(f"{invalid_tpu} invalid TPU values found")
            
            tpu_out_of_range = ((df['tpu'] < self.valid_tpu_range[0]) | 
                               (df['tpu'] > self.valid_tpu_range[1])).sum()
            if tpu_out_of_range > 0:
                validation_results['warnings'].append(f"{tpu_out_of_range} TPU values out of range")
            
            # Validate reactor column
            df['reactor'] = pd.to_numeric(df['reactor'], errors='coerce')
            invalid_reactor = df['reactor'].isna().sum()
            if invalid_reactor > 0:
                validation_results['warnings'].append(f"{invalid_reactor} invalid reactor values found")
            
            reactor_out_of_range = ((df['reactor'] < self.valid_reactor_range[0]) | 
                                   (df['reactor'] > self.valid_reactor_range[1])).sum()
            if reactor_out_of_range > 0:
                validation_results['warnings'].append(f"{reactor_out_of_range} reactor values out of range")
            
            # Validate algae_density column
            df['algae_density'] = pd.to_numeric(df['algae_density'], errors='coerce')
            invalid_density = df['algae_density'].isna().sum()
            if invalid_density > 0:
                validation_results['warnings'].append(f"{invalid_density} invalid density values found")
            
            density_out_of_range = ((df['algae_density'] < self.valid_density_range[0]) | 
                                   (df['algae_density'] > self.valid_density_range[1])).sum()
            if density_out_of_range > 0:
                validation_results['warnings'].append(f"{density_out_of_range} density values out of range")
            
        except Exception as e:
            validation_results['is_valid'] = False
            validation_results['errors'].append(f"Data validation error: {e}")
        
        return validation_results
    
    def clean_flowcam_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize FlowCam data.
        
        Args:
            df: Raw DataFrame to clean.
            
        Returns:
            Cleaned DataFrame.
        """
        try:
            # Create a copy to avoid modifying original
            cleaned_df = df.copy()
            
            # Convert date column
            cleaned_df['date'] = pd.to_datetime(cleaned_df['date'], errors='coerce')
            
            # Convert numeric columns
            cleaned_df['tpu'] = pd.to_numeric(cleaned_df['tpu'], errors='coerce')
            cleaned_df['reactor'] = pd.to_numeric(cleaned_df['reactor'], errors='coerce')
            cleaned_df['algae_density'] = pd.to_numeric(cleaned_df['algae_density'], errors='coerce')
            
            # Remove rows with invalid data
            initial_rows = len(cleaned_df)
            cleaned_df = cleaned_df.dropna(subset=['date', 'tpu', 'reactor', 'algae_density'])
            removed_rows = initial_rows - len(cleaned_df)
            
            if removed_rows > 0:
                logger.warning(f"Removed {removed_rows} rows with invalid data")
            
            # Filter out-of-range values
            cleaned_df = cleaned_df[
                (cleaned_df['tpu'] >= self.valid_tpu_range[0]) &
                (cleaned_df['tpu'] <= self.valid_tpu_range[1]) &
                (cleaned_df['reactor'] >= self.valid_reactor_range[0]) &
                (cleaned_df['reactor'] <= self.valid_reactor_range[1]) &
                (cleaned_df['algae_density'] >= self.valid_density_range[0]) &
                (cleaned_df['algae_density'] <= self.valid_density_range[1])
            ]
            
            # Sort by date and TPU
            cleaned_df = cleaned_df.sort_values(['date', 'tpu', 'reactor'])
            
            # Reset index
            cleaned_df = cleaned_df.reset_index(drop=True)
            
            logger.info(f"Cleaned FlowCam data: {len(cleaned_df)} rows remaining")
            return cleaned_df
            
        except Exception as e:
            logger.error(f"Failed to clean FlowCam data: {e}")
            return df
    
    def add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived columns to FlowCam data.
        
        Args:
            df: DataFrame to enhance.
            
        Returns:
            DataFrame with additional derived columns.
        """
        try:
            enhanced_df = df.copy()
            
            # Add date components
            enhanced_df['year'] = enhanced_df['date'].dt.year
            enhanced_df['month'] = enhanced_df['date'].dt.month
            enhanced_df['day'] = enhanced_df['date'].dt.day
            enhanced_df['day_of_week'] = enhanced_df['date'].dt.dayofweek
            enhanced_df['week_of_year'] = enhanced_df['date'].dt.isocalendar().week
            
            # Add density categories
            enhanced_df['density_category'] = pd.cut(
                enhanced_df['algae_density'],
                bins=[0, 0.5, 1.0, 1.5, 2.0, 3.0],
                labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'],
                include_lowest=True
            )
            
            # Add growth indicators (if multiple dates per TPU/reactor)
            enhanced_df = enhanced_df.sort_values(['tpu', 'reactor', 'date'])
            enhanced_df['density_change'] = enhanced_df.groupby(['tpu', 'reactor'])['algae_density'].diff()
            enhanced_df['density_change_pct'] = enhanced_df.groupby(['tpu', 'reactor'])['algae_density'].pct_change() * 100
            
            logger.info("Added derived columns to FlowCam data")
            return enhanced_df
            
        except Exception as e:
            logger.error(f"Failed to add derived columns: {e}")
            return df
    
    def calculate_daily_aggregates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate daily aggregates for FlowCam data.
        
        Args:
            df: DataFrame with FlowCam data.
            
        Returns:
            DataFrame with daily aggregates.
        """
        try:
            # Group by date, TPU, and reactor
            daily_agg = df.groupby(['date', 'tpu', 'reactor']).agg({
                'algae_density': ['mean', 'min', 'max', 'std', 'count']
            }).round(4)
            
            # Flatten column names
            daily_agg.columns = ['_'.join(col).strip() for col in daily_agg.columns]
            daily_agg = daily_agg.reset_index()
            
            # Rename columns for clarity
            daily_agg = daily_agg.rename(columns={
                'algae_density_mean': 'avg_density',
                'algae_density_min': 'min_density',
                'algae_density_max': 'max_density',
                'algae_density_std': 'std_density',
                'algae_density_count': 'measurement_count'
            })
            
            logger.info(f"Calculated daily aggregates for {len(daily_agg)} records")
            return daily_agg
            
        except Exception as e:
            logger.error(f"Failed to calculate daily aggregates: {e}")
            return pd.DataFrame()
    
    def process_flowcam_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Complete FlowCam data processing pipeline.
        
        Args:
            file_path: Path to the FlowCam CSV file.
            
        Returns:
            Processed DataFrame or None if error.
        """
        try:
            # Read data
            df = self.read_flowcam_csv(file_path)
            if df is None:
                return None
            
            # Validate data
            validation = self.validate_flowcam_data(df)
            if not validation['is_valid']:
                logger.error(f"Data validation failed: {validation['errors']}")
                return None
            
            if validation['warnings']:
                logger.warning(f"Data validation warnings: {validation['warnings']}")
            
            # Clean data
            cleaned_df = self.clean_flowcam_data(df)
            
            # Add derived columns
            enhanced_df = self.add_derived_columns(cleaned_df)
            
            logger.info(f"Successfully processed FlowCam file: {file_path}")
            return enhanced_df
            
        except Exception as e:
            logger.error(f"Failed to process FlowCam file {file_path}: {e}")
            return None
