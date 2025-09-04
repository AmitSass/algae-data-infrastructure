#!/usr/bin/env python3
# SECURITY NOTE: synthetic/demo only. No production endpoints, secrets, or schemas.

"""
Weather Data Processing Example - Based on Production Code
=========================================================

This is a cleaned and sanitized example of the weather data processing logic
from the production BarAlgae system. All sensitive information has been removed
and replaced with environment variables for demonstration purposes.

Original functionality:
- Fetches historical weather data from Open-Meteo API
- Processes weather parameters (temperature, humidity, radiation, etc.)
- Uploads to S3 with proper partitioning (Bronze layer)
- Handles timezone conversions and data validation
- Supports both historical and forecast data

Security Note: This example contains no production secrets or real data.
"""

import requests
import pandas as pd
import datetime
import pytz
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
from typing import Dict, List, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherProcessor:
    """
    Processes weather data from external APIs.
    
    This class handles the complete pipeline from API data fetching
    to S3 upload with proper data transformation and validation.
    """
    
    def __init__(self, s3_client, bucket_name: str, 
                 latitude: float = None, longitude: float = None,
                 city: str = None):
        """
        Initialize weather processor.
        
        Args:
            s3_client: Configured S3 client
            bucket_name: Target S3 bucket name
            latitude: Location latitude (from ENV if None)
            longitude: Location longitude (from ENV if None)
            city: Location city name (from ENV if None)
        """
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.latitude = latitude or float(os.getenv("DEMO_LAT", "32.0"))
        self.longitude = longitude or float(os.getenv("DEMO_LON", "35.0"))
        self.city = city or os.getenv("DEMO_CITY", "Demo City, IL")
        
        # Weather API configuration
        self.api_base_url = "https://archive-api.open-meteo.com/v1/archive"
        self.forecast_url = "https://api.open-meteo.com/v1/forecast"
        
        # Weather parameters to fetch
        self.hourly_params = [
            "temperature_2m", 
            "relative_humidity_2m", 
            "cloudcover", 
            "windspeed_10m",
            "shortwave_radiation", 
            "direct_normal_irradiance"
        ]
    
    def get_historical_weather(self, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        Fetch historical weather data from API.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with weather data or None if error
        """
        try:
            params = {
                "latitude": self.latitude,
                "longitude": self.longitude,
                "start_date": start_date,
                "end_date": end_date,
                "hourly": self.hourly_params,
                "timezone": "auto"
            }
            
            logger.info(f"Fetching historical weather data from {start_date} to {end_date}")
            
            response = requests.get(self.api_base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Validate response
            if "hourly" not in data or "time" not in data["hourly"]:
                logger.error("No valid data received from weather API")
                return None
            
            # Create DataFrame
            df = pd.DataFrame({
                "timestamp": data['hourly']['time'],
                "Solar Radiation (W/m²)": data['hourly']['shortwave_radiation'],
                "Photon Flux (µmol/m²/s)": data['hourly']['direct_normal_irradiance'],
                "Temperature (°C)": data['hourly']['temperature_2m'],
                "Humidity (%)": data['hourly']['relative_humidity_2m'],
                "Wind Speed (km/h)": data['hourly']['windspeed_10m'],
                "Cloud Cover (%)": data['hourly']['cloudcover']
            })
            
            # Add metadata
            df['latitude'] = self.latitude
            df['longitude'] = self.longitude
            df['city'] = self.city
            df['data_type'] = 'historical'
            df['fetch_timestamp'] = datetime.datetime.now(pytz.utc).isoformat()
            
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Add date partitioning columns
            df['year'] = df['timestamp'].dt.year.astype(str)
            df['month'] = df['timestamp'].dt.month.astype(str).str.zfill(2)
            df['day'] = df['timestamp'].dt.day.astype(str).str.zfill(2)
            df['hour'] = df['timestamp'].dt.hour.astype(str).str.zfill(2)
            
            logger.info(f"Fetched {len(df)} weather records")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching historical weather data: {str(e)}")
            return None
    
    def get_weather_forecast(self, days_ahead: int = 3) -> Optional[pd.DataFrame]:
        """
        Fetch weather forecast data from API.
        
        Args:
            days_ahead: Number of days to forecast
            
        Returns:
            DataFrame with forecast data or None if error
        """
        try:
            params = {
                "latitude": self.latitude,
                "longitude": self.longitude,
                "hourly": self.hourly_params,
                "forecast_days": days_ahead,
                "timezone": "auto"
            }
            
            logger.info(f"Fetching weather forecast for {days_ahead} days ahead")
            
            response = requests.get(self.forecast_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Validate response
            if "hourly" not in data or "time" not in data["hourly"]:
                logger.error("No valid forecast data received from weather API")
                return None
            
            # Create DataFrame
            df = pd.DataFrame({
                "timestamp": data['hourly']['time'],
                "Solar Radiation (W/m²)": data['hourly']['shortwave_radiation'],
                "Photon Flux (µmol/m²/s)": data['hourly']['direct_normal_irradiance'],
                "Temperature (°C)": data['hourly']['temperature_2m'],
                "Humidity (%)": data['hourly']['relative_humidity_2m'],
                "Wind Speed (km/h)": data['hourly']['windspeed_10m'],
                "Cloud Cover (%)": data['hourly']['cloudcover']
            })
            
            # Add metadata
            df['latitude'] = self.latitude
            df['longitude'] = self.longitude
            df['city'] = self.city
            df['data_type'] = 'forecast'
            df['fetch_timestamp'] = datetime.datetime.now(pytz.utc).isoformat()
            
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Add date partitioning columns
            df['year'] = df['timestamp'].dt.year.astype(str)
            df['month'] = df['timestamp'].dt.month.astype(str).str.zfill(2)
            df['day'] = df['timestamp'].dt.day.astype(str).str.zfill(2)
            df['hour'] = df['timestamp'].dt.hour.astype(str).str.zfill(2)
            
            logger.info(f"Fetched {len(df)} forecast records")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching weather forecast: {str(e)}")
            return None
    
    def upload_to_s3(self, df: pd.DataFrame, data_type: str, 
                    year: str, month: str, day: str) -> Optional[str]:
        """
        Upload weather data to S3.
        
        Args:
            df: Weather DataFrame
            data_type: Type of data (historical/forecast)
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
            s3_key = f"bronze/weather/data_type={data_type}/year={year}/month={month}/day={day}/weather_data.parquet"
            
            # Upload to S3
            self.s3_client.upload_fileobj(
                buffer, 
                self.bucket_name, 
                s3_key
            )
            
            logger.info(f"Successfully uploaded {data_type} weather data to s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Error uploading weather data to S3: {str(e)}")
            return None
    
    def process_historical_data(self, days_back: int = 2) -> List[str]:
        """
        Process historical weather data for specified number of days.
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            List of uploaded S3 keys
        """
        uploaded_keys = []
        
        # Calculate date range
        today = datetime.datetime.now(pytz.utc)
        start_date = (today - datetime.timedelta(days=days_back)).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
        
        # Fetch data
        df = self.get_historical_weather(start_date, end_date)
        if df is None or df.empty:
            return uploaded_keys
        
        # Upload each day separately
        for date in df['timestamp'].dt.date.unique():
            day_df = df[df['timestamp'].dt.date == date]
            if not day_df.empty:
                year, month, day = date.strftime("%Y-%m-%d").split("-")
                s3_key = self.upload_to_s3(day_df, 'historical', year, month, day)
                if s3_key:
                    uploaded_keys.append(s3_key)
        
        return uploaded_keys
    
    def process_forecast_data(self, days_ahead: int = 3) -> List[str]:
        """
        Process weather forecast data.
        
        Args:
            days_ahead: Number of days to forecast
            
        Returns:
            List of uploaded S3 keys
        """
        uploaded_keys = []
        
        # Fetch forecast data
        df = self.get_weather_forecast(days_ahead)
        if df is None or df.empty:
            return uploaded_keys
        
        # Upload each day separately
        for date in df['timestamp'].dt.date.unique():
            day_df = df[df['timestamp'].dt.date == date]
            if not day_df.empty:
                year, month, day = date.strftime("%Y-%m-%d").split("-")
                s3_key = self.upload_to_s3(day_df, 'forecast', year, month, day)
                if s3_key:
                    uploaded_keys.append(s3_key)
        
        return uploaded_keys


def main():
    """
    Example usage of weather processor.
    
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
    
    # Initialize processor (coordinates from ENV)
    processor = WeatherProcessor(s3_client, bucket_name)
    
    # Process historical data
    historical_keys = processor.process_historical_data(days_back=2)
    print(f"Processed {len(historical_keys)} days of historical weather data")
    
    # Process forecast data
    forecast_keys = processor.process_forecast_data(days_ahead=3)
    print(f"Processed {len(forecast_keys)} days of forecast weather data")
    
    # Print all uploaded keys
    all_keys = historical_keys + forecast_keys
    for key in all_keys:
        print(f"  - {key}")


if __name__ == "__main__":
    main()
