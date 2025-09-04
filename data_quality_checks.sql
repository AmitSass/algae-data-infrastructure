-- Data Quality Checks for BarAlgae Infrastructure
-- Comprehensive SQL queries for data validation

-- 1. Data Completeness Check
SELECT 
    'flowcam_data' as table_name,
    count(*) as total_records,
    count(measurement_timestamp) as non_null_timestamps,
    count(cell_density) as non_null_density,
    (count(measurement_timestamp)::float / count(*) * 100) as completeness_pct
FROM stg_flowcam__raw
UNION ALL
SELECT 
    'scada_data' as table_name,
    count(*) as total_records,
    count(timestamp) as non_null_timestamps,
    count(temperature) as non_null_temp,
    (count(timestamp)::float / count(*) * 100) as completeness_pct
FROM stg_scada__citect;

-- 2. Data Range Validation
SELECT 
    'flowcam_density' as metric,
    min(cell_density) as min_value,
    max(cell_density) as max_value,
    avg(cell_density) as avg_value,
    stddev(cell_density) as std_dev
FROM stg_flowcam__raw
WHERE cell_density IS NOT NULL
UNION ALL
SELECT 
    'scada_temperature' as metric,
    min(temperature) as min_value,
    max(temperature) as max_value,
    avg(temperature) as avg_value,
    stddev(temperature) as std_dev
FROM stg_scada__citect
WHERE temperature IS NOT NULL;

-- 3. Duplicate Detection
SELECT 
    measurement_timestamp,
    cell_density,
    count(*) as duplicate_count
FROM stg_flowcam__raw
GROUP BY measurement_timestamp, cell_density
HAVING count(*) > 1
ORDER BY duplicate_count DESC;

-- 4. Data Freshness Check
SELECT 
    'flowcam' as data_source,
    max(measurement_timestamp) as latest_record,
    current_timestamp - max(measurement_timestamp) as time_since_last_update,
    case 
        when current_timestamp - max(measurement_timestamp) < interval '1 hour' then 'Fresh'
        when current_timestamp - max(measurement_timestamp) < interval '24 hours' then 'Stale'
        else 'Very Stale'
    end as freshness_status
FROM stg_flowcam__raw
UNION ALL
SELECT 
    'scada' as data_source,
    max(timestamp) as latest_record,
    current_timestamp - max(timestamp) as time_since_last_update,
    case 
        when current_timestamp - max(timestamp) < interval '1 hour' then 'Fresh'
        when current_timestamp - max(timestamp) < interval '24 hours' then 'Stale'
        else 'Very Stale'
    end as freshness_status
FROM stg_scada__citect;

-- 5. Data Consistency Check
SELECT 
    date_trunc('day', measurement_timestamp) as check_date,
    count(*) as record_count,
    count(distinct measurement_timestamp) as unique_timestamps,
    case 
        when count(*) = count(distinct measurement_timestamp) then 'Consistent'
        else 'Inconsistent - Duplicates Found'
    end as consistency_status
FROM stg_flowcam__raw
GROUP BY 1
ORDER BY 1 DESC;
