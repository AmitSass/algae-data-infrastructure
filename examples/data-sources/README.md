# Data Sources Examples

This directory contains sanitized examples of the data processing logic from the production BarAlgae system. These examples demonstrate the real-world data engineering patterns and techniques used in the actual production environment, but with all sensitive information removed and replaced with placeholder values.

## 🔒 Security Note

**These examples contain NO production secrets, real data, or sensitive information.** All credentials, API keys, database connections, and actual data have been replaced with placeholder values for demonstration purposes only.

## 📁 Available Examples

### 1. FlowCam Data Processing
**File**: `flowcam_processor_example.py`

**Original Functionality**:
- Processes FlowCam microscopy data files (.lst format)
- Handles multiple algae types (pt, nano, algae health, TS, TW)
- Extracts TPU and reactor information from folder structure
- Uploads to S3 with proper partitioning (Bronze layer)
- Converts to Parquet format for efficient storage

**Key Features**:
- File discovery and validation
- Data parsing and transformation
- S3 partitioning strategy
- Error handling and logging

### 2. SCADA Data Processing
**File**: `scada_processor_example.py`

**Original Functionality**:
- Connects to SQL Server database (CitectData)
- Extracts SCADA sensor data for specified date ranges
- Performs data transformation and unpivoting
- Handles multiple sensor parameters (CO2, temperature, pH, etc.)
- Uploads to S3 with proper partitioning

**Key Features**:
- Database connection management
- Data unpivoting and transformation
- Date range processing
- Column type conversions

### 3. Weather Data Processing
**File**: `weather_processor_example.py`

**Original Functionality**:
- Fetches historical weather data from Open-Meteo API
- Processes weather parameters (temperature, humidity, radiation, etc.)
- Handles timezone conversions and data validation
- Supports both historical and forecast data
- Uploads to S3 with proper partitioning

**Key Features**:
- API integration and error handling
- Timezone management
- Data validation and cleaning
- Historical and forecast data processing

### 4. Growth Tracking Data Processing
**File**: `growth_processor_example.py`

**Original Functionality**:
- Downloads data from Google Drive using service account
- Processes multiple TPU sheets (TPU2, TPU3, TPU4, TPU5)
- Translates Hebrew column names to English
- Performs data cleaning and validation
- Uploads to S3 with proper partitioning

**Key Features**:
- Google Drive API integration
- Multi-language column mapping
- Data type conversions
- TPU-specific processing

### 5. Harvest Documentation Processing
**File**: `harvest_processor_example.py`

**Original Functionality**:
- Processes harvest documentation Excel files from network volumes
- Performs data cleaning and column renaming
- Handles file permissions and network access
- Validates data integrity and completeness
- Uploads to S3 with proper partitioning

**Key Features**:
- Network file system access
- File permission validation
- Data integrity checks
- Column standardization

## 🏗️ Architecture Patterns

These examples demonstrate several important data engineering patterns:

### 1. **Medallion Architecture**
- **Bronze Layer**: Raw data ingestion with minimal processing
- **Silver Layer**: Data cleaning and standardization
- **Gold Layer**: Business-ready analytics tables

### 2. **S3 Partitioning Strategy**
```
bronze/
├── flowcam/
│   └── algae={type}/
│       └── year={year}/
│           └── month={month}/
│               └── day={day}/
│                   └── tpu={tpu}/
│                       └── reactor={reactor}/
├── scada/
│   └── year={year}/
│       └── month={month}/
│           └── day={day}/
├── weather/
│   └── data_type={historical|forecast}/
│       └── year={year}/
│           └── month={month}/
│               └── day={day}/
├── growth/
│   └── tpu={tpu}/
│       └── year={year}/
│           └── month={month}/
│               └── day={day}/
└── harvest/
    └── year={year}/
        └── month={month}/
            └── day={day}/
```

### 3. **Data Processing Pipeline**
1. **Extract**: Data source connection and retrieval
2. **Transform**: Data cleaning, validation, and standardization
3. **Load**: S3 upload with proper partitioning
4. **Validate**: Data quality checks and error handling

### 4. **Error Handling Patterns**
- Comprehensive logging at all levels
- Graceful error recovery
- Data validation and quality checks
- Retry mechanisms for transient failures

## 🚀 Usage Examples

### Running Individual Processors

```python
# FlowCam processing
from flowcam_processor_example import FlowCamProcessor

processor = FlowCamProcessor(s3_client, bucket_name)
uploaded_keys = processor.process_directory("/path/to/flowcam/data")

# SCADA processing
from scada_processor_example import SCADAProcessor

processor = SCADAProcessor(s3_client, bucket_name, db_connection_string)
uploaded_keys = processor.process_multiple_days(num_days=2)

# Weather processing
from weather_processor_example import WeatherProcessor

processor = WeatherProcessor(s3_client, bucket_name)
historical_keys = processor.process_historical_data(days_back=2)
forecast_keys = processor.process_forecast_data(days_ahead=3)
```

### Integration with Airflow

These processors are designed to be used as Airflow tasks:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator

def flowcam_task():
    processor = FlowCamProcessor(s3_client, bucket_name)
    return processor.process_directory("/opt/airflow/volumes")

dag = DAG('data_ingestion', ...)
flowcam_op = PythonOperator(
    task_id='process_flowcam',
    python_callable=flowcam_task,
    dag=dag
)
```

## 🔧 Configuration

All examples use environment variables and configuration files for sensitive information:

```python
# Example configuration
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION', 'us-east-1')
)
```

## 📊 Data Quality

Each processor includes data quality checks:

- **Schema validation**: Required columns and data types
- **Range validation**: Numeric value ranges and constraints
- **Completeness checks**: Missing value detection
- **Consistency checks**: Cross-field validation
- **Freshness checks**: Data age validation

## 🧪 Testing

The examples include comprehensive error handling and can be easily tested:

```python
# Unit testing example
def test_flowcam_processor():
    processor = FlowCamProcessor(mock_s3_client, "test-bucket")
    result = processor.extract_tpu_reactor_info("1.02")
    assert result == ("1", "02")
```

## 📈 Monitoring

Each processor includes detailed logging for monitoring:

- **Processing metrics**: Records processed, errors encountered
- **Performance metrics**: Processing time, data volume
- **Quality metrics**: Validation failures, data quality issues
- **Operational metrics**: S3 upload success/failure rates

## 🔄 Production Considerations

When adapting these examples for production:

1. **Security**: Use proper secret management (AWS Secrets Manager, etc.)
2. **Scalability**: Implement proper error handling and retry logic
3. **Monitoring**: Add comprehensive logging and alerting
4. **Testing**: Implement unit and integration tests
5. **Documentation**: Maintain up-to-date documentation

## 📚 Related Documentation

- [Main README](../../README.md) - Project overview
- [S3 Partitioning Guide](../../docs/S3_PARTITIONING.md) - Detailed partitioning strategy
- [Architecture Diagrams](../../docs/diagrams/architecture.mmd) - System architecture
- [dbt Models](../../transform/dbt/models/) - Data transformation models
- [Great Expectations](../../data_quality/great_expectations/) - Data quality framework

---

**Note**: These examples are for demonstration purposes only and should not be used in production without proper security review and testing.
