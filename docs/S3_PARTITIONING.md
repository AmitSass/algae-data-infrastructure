# S3 Partitioning Conventions

This document describes the S3 partitioning strategy used in the BarAlgae Data Infrastructure project.

## Overview

The project follows a **Medallion Architecture** with three layers:
- **Bronze**: Raw data ingestion
- **Silver**: Standardized and cleaned data
- **Gold**: Business-ready analytics data

## Partitioning Strategy

### Bronze Layer (Raw Data)

```
s3://baralgae-bronze/
├── flowcam/
│   └── date=YYYY-MM-DD/
│       ├── TPU=01/
│       │   ├── reactor=01/
│       │   │   └── flowcam_data.csv
│       │   ├── reactor=02/
│       │   │   └── flowcam_data.csv
│       │   └── ...
│       ├── TPU=02/
│       └── ...
├── scada/
│   └── date=YYYY-MM-DD/
│       ├── citect/
│       │   └── scada_citect.csv
│       └── status/
│           └── scada_status.csv
├── weather/
│   └── date=YYYY-MM-DD/
│       ├── historical/
│       │   └── weather_historical.csv
│       └── forecast/
│           └── weather_forecast.csv
└── growth_tracking/
    └── date=YYYY-MM-DD/
        └── growth_data.csv
```

### Silver Layer (Standardized Data)

```
s3://baralgae-silver/
├── flowcam/
│   └── date=YYYY-MM-DD/
│       ├── tpu_id=01/
│       │   ├── reactor_id=01/
│       │   │   └── flowcam_standardized.parquet
│       │   └── ...
│       └── ...
├── scada/
│   └── date=YYYY-MM-DD/
│       └── scada_standardized.parquet
├── weather/
│   └── date=YYYY-MM-DD/
│       └── weather_standardized.parquet
└── growth_tracking/
    └── date=YYYY-MM-DD/
        └── growth_standardized.parquet
```

### Gold Layer (Analytics Data)

```
s3://baralgae-gold/
├── analytics/
│   └── date=YYYY-MM-DD/
│       ├── flowcam_daily_summary.parquet
│       ├── scada_daily_summary.parquet
│       ├── weather_daily_summary.parquet
│       └── growth_daily_summary.parquet
├── marts/
│   └── date=YYYY-MM-DD/
│       ├── fact_flowcam_summary.parquet
│       ├── fact_scada_summary.parquet
│       ├── dim_tpu.parquet
│       └── dim_reactor.parquet
└── ml_features/
    └── date=YYYY-MM-DD/
        ├── flowcam_features.parquet
        ├── scada_features.parquet
        └── combined_features.parquet
```

## Partitioning Benefits

### Query Performance
- **Partition Pruning**: Queries only scan relevant partitions
- **Columnar Storage**: Parquet format for efficient analytics
- **Compression**: Reduced storage costs and faster I/O

### Data Management
- **Retention Policies**: Easy to implement time-based retention
- **Access Control**: Granular permissions per partition
- **Cost Optimization**: Different storage classes per layer

### Data Lineage
- **Clear Hierarchy**: Easy to understand data flow
- **Audit Trail**: Track data transformations across layers
- **Versioning**: Support for data versioning and rollback

## Implementation Examples

### Python (boto3)

```python
def upload_to_bronze(data, date, tpu_id, reactor_id):
    """Upload data to Bronze layer with proper partitioning."""
    s3_key = f"flowcam/date={date}/TPU={tpu_id:02d}/reactor={reactor_id:02d}/flowcam_data.csv"
    s3_client.upload_file(data, "baralgae-bronze", s3_key)

def upload_to_silver(data, date, tpu_id, reactor_id):
    """Upload data to Silver layer with proper partitioning."""
    s3_key = f"flowcam/date={date}/tpu_id={tpu_id:02d}/reactor_id={reactor_id:02d}/flowcam_standardized.parquet"
    s3_client.upload_file(data, "baralgae-silver", s3_key)
```

### SQL (Redshift)

```sql
-- Create partitioned table
CREATE TABLE flowcam_daily_summary (
    measurement_date DATE,
    tpu_id INTEGER,
    reactor_id INTEGER,
    avg_density DECIMAL(10,3),
    -- ... other columns
)
PARTITION BY RANGE (measurement_date);

-- Query with partition pruning
SELECT * FROM flowcam_daily_summary
WHERE measurement_date >= '2024-01-01'
  AND measurement_date < '2024-02-01';
```

### dbt

```sql
-- models/gold/gd_flowcam__daily_summary.sql
{{ config(
    materialized='table',
    partition_by={
        'field': 'measurement_date',
        'data_type': 'date',
        'granularity': 'day'
    }
) }}

select
    measurement_date,
    tpu_id,
    reactor_id,
    avg_density,
    -- ... other columns
from {{ ref('sl_flowcam__standardized') }}
group by measurement_date, tpu_id, reactor_id
```

## Best Practices

### Naming Conventions
- **Lowercase**: All directory and file names in lowercase
- **Underscores**: Use underscores for multi-word names
- **Zero Padding**: Use zero-padded numbers (01, 02, etc.)
- **Descriptive**: Use descriptive names for data types

### File Formats
- **Bronze**: CSV for raw data ingestion
- **Silver**: Parquet for standardized data
- **Gold**: Parquet for analytics data

### Compression
- **Gzip**: For CSV files
- **Snappy**: For Parquet files (default)
- **Zstd**: For high compression ratio when needed

### Monitoring
- **Partition Health**: Monitor partition completeness
- **File Sizes**: Track file size distribution
- **Query Performance**: Monitor query execution times
- **Storage Costs**: Track storage usage by layer

## Migration Strategy

### From Legacy Structure
1. **Inventory**: Catalog existing data structure
2. **Plan**: Design new partitioning strategy
3. **Migrate**: Move data to new structure
4. **Validate**: Ensure data integrity
5. **Update**: Modify applications to use new structure

### Rollback Plan
1. **Backup**: Keep original data structure
2. **Validation**: Test new structure thoroughly
3. **Gradual**: Migrate data incrementally
4. **Monitoring**: Watch for issues during migration

## Troubleshooting

### Common Issues
- **Missing Partitions**: Check data ingestion pipeline
- **Large Files**: Consider splitting large files
- **Query Timeouts**: Optimize partition pruning
- **Storage Costs**: Review retention policies

### Performance Tuning
- **Partition Size**: Aim for 128MB-1GB per partition
- **File Count**: Limit files per partition
- **Compression**: Choose appropriate compression
- **Indexing**: Use appropriate indexes for queries

## Security Considerations

### Access Control
- **IAM Policies**: Restrict access by partition
- **Encryption**: Encrypt data at rest and in transit
- **Audit Logging**: Log all data access
- **Data Classification**: Tag sensitive data

### Compliance
- **GDPR**: Implement data retention policies
- **SOX**: Maintain audit trails
- **HIPAA**: Ensure data privacy
- **Industry Standards**: Follow relevant regulations

---

For more information, see the [main README](../README.md) or contact the data engineering team.
