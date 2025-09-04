#!/usr/bin/env python3
"""
Demo data seeding script for BarAlgae data infrastructure.

This script generates synthetic FlowCam data for demonstration purposes.
The data simulates algae density measurements across multiple TPUs and reactors.
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
import random
import pandas as pd

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from algae_lib import S3Manager, DatabaseManager, FlowCamProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_flowcam_data(days: int = 14, tpus: int = 3, reactors_per_tpu: int = 5) -> pd.DataFrame:
    """
    Generate synthetic FlowCam data.
    
    Args:
        days: Number of days to generate data for.
        tpus: Number of TPUs (Tank Processing Units).
        reactors_per_tpu: Number of reactors per TPU.
        
    Returns:
        DataFrame with synthetic FlowCam data.
    """
    logger.info(f"Generating {days} days of data for {tpus} TPUs with {reactors_per_tpu} reactors each")
    
    data = []
    start_date = datetime.now() - timedelta(days=days)
    
    for day in range(days):
        current_date = start_date + timedelta(days=day)
        
        for tpu in range(1, tpus + 1):
            for reactor in range(1, reactors_per_tpu + 1):
                # Generate realistic algae density values
                # Base density varies by TPU and reactor
                base_density = 0.8 + (tpu * 0.1) + (reactor * 0.05)
                
                # Add some daily variation
                daily_variation = random.uniform(-0.2, 0.2)
                
                # Add some random noise
                noise = random.uniform(-0.1, 0.1)
                
                # Calculate final density
                density = max(0.1, min(2.5, base_density + daily_variation + noise))
                
                data.append({
                    'date': current_date.strftime('%Y-%m-%d'),
                    'tpu': tpu,
                    'reactor': reactor,
                    'algae_density': round(density, 3)
                })
    
    df = pd.DataFrame(data)
    logger.info(f"Generated {len(df)} records")
    return df


def save_demo_data(df: pd.DataFrame, output_path: str) -> bool:
    """
    Save demo data to CSV file.
    
    Args:
        df: DataFrame to save.
        output_path: Path where to save the CSV file.
        
    Returns:
        True if successful.
    """
    try:
        # Ensure directory exists
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Save to CSV
        df.to_csv(output_path, index=False)
        logger.info(f"Saved demo data to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to save demo data: {e}")
        return False


def upload_to_s3(df: pd.DataFrame, s3_manager: S3Manager) -> bool:
    """
    Upload demo data to S3.
    
    Args:
        df: DataFrame to upload.
        s3_manager: S3Manager instance.
        
    Returns:
        True if successful.
    """
    try:
        # Create temporary CSV file
        temp_path = "temp_flowcam_demo.csv"
        df.to_csv(temp_path, index=False)
        
        # Generate S3 key with current date
        today = datetime.now().strftime('%Y-%m-%d')
        s3_key = f"bronze/flowcam/date={today}/flowcam_sample.csv"
        
        # Upload to S3
        success = s3_manager.upload_file(temp_path, s3_key)
        
        # Clean up temporary file
        if os.path.exists(temp_path):
            os.remove(temp_path)
        
        if success:
            logger.info(f"Uploaded demo data to S3: s3://{s3_manager.bucket}/{s3_key}")
        else:
            logger.error("Failed to upload demo data to S3")
        
        return success
    except Exception as e:
        logger.error(f"Failed to upload to S3: {e}")
        return False


def load_to_database(df: pd.DataFrame, db_manager: DatabaseManager) -> bool:
    """
    Load demo data to database.
    
    Args:
        df: DataFrame to load.
        db_manager: DatabaseManager instance.
        
    Returns:
        True if successful.
    """
    try:
        # Test database connection
        if not db_manager.test_connection():
            logger.error("Database connection test failed")
            return False
        
        # Load data to database
        success = db_manager.load_dataframe_to_table(
            df, 
            "flowcam_raw", 
            if_exists="replace"
        )
        
        if success:
            logger.info("Loaded demo data to database table 'flowcam_raw'")
        else:
            logger.error("Failed to load demo data to database")
        
        return success
    except Exception as e:
        logger.error(f"Failed to load to database: {e}")
        return False


def main():
    """Main function to generate and seed demo data."""
    logger.info("Starting demo data seeding process")
    
    try:
        # Generate synthetic data
        df = generate_flowcam_data(days=14, tpus=3, reactors_per_tpu=5)
        
        # Save to local CSV file
        output_path = "examples/data-sample/flowcam_sample.csv"
        if not save_demo_data(df, output_path):
            logger.error("Failed to save demo data locally")
            return False
        
        # Initialize managers
        s3_manager = S3Manager()
        db_manager = DatabaseManager()
        
        # Upload to S3 (if S3 is available)
        try:
            upload_to_s3(df, s3_manager)
        except Exception as e:
            logger.warning(f"S3 upload failed (this is OK for local development): {e}")
        
        # Load to database (if database is available)
        try:
            load_to_database(df, db_manager)
        except Exception as e:
            logger.warning(f"Database load failed (this is OK for local development): {e}")
        
        logger.info("Demo data seeding completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Demo data seeding failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
