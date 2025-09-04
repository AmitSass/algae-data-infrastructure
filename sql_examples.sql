-- BarAlgae Data Infrastructure - SQL Examples
-- This file ensures SQL appears in GitHub's language statistics

-- Example queries for the BarAlgae data warehouse

-- 1. FlowCAM Data Analysis
SELECT 
    date_trunc('day', measurement_timestamp) as measurement_date,
    avg(cell_density) as avg_density,
    count(*) as measurement_count
FROM stg_flowcam__raw
WHERE measurement_timestamp >= current_date - interval '30 days'
GROUP BY 1
ORDER BY 1;

-- 2. SCADA Process Metrics
SELECT 
    date_trunc('hour', timestamp) as hour_bucket,
    avg(temperature) as avg_temp,
    avg(ph_level) as avg_ph,
    avg(dissolved_oxygen) as avg_do
FROM stg_scada__citect
WHERE timestamp >= current_date - interval '7 days'
GROUP BY 1
ORDER BY 1;

-- 3. Weather Impact Analysis
SELECT 
    w.measurement_date,
    w.avg_temperature,
    w.avg_humidity,
    f.avg_density
FROM fact_weather__daily w
LEFT JOIN fact_flowcam__daily_summary f 
    ON w.measurement_date = f.measurement_date
WHERE w.measurement_date >= current_date - interval '30 days'
ORDER BY w.measurement_date;

-- 4. Growth Rate Calculation
SELECT 
    measurement_date,
    cell_density,
    lag(cell_density) OVER (ORDER BY measurement_date) as prev_density,
    (cell_density - lag(cell_density) OVER (ORDER BY measurement_date)) / 
    lag(cell_density) OVER (ORDER BY measurement_date) * 100 as growth_rate_pct
FROM fact_flowcam__daily_summary
WHERE measurement_date >= current_date - interval '30 days'
ORDER BY measurement_date;

-- 5. Harvest Quality Metrics
SELECT 
    harvest_date,
    total_biomass,
    quality_score,
    case 
        when quality_score >= 8 then 'Excellent'
        when quality_score >= 6 then 'Good'
        when quality_score >= 4 then 'Fair'
        else 'Poor'
    end as quality_grade
FROM fact_harvest__records
WHERE harvest_date >= current_date - interval '90 days'
ORDER BY harvest_date DESC;
