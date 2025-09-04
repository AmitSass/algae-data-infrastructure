-- Intermediate model for daily FlowCam aggregations
-- This model aggregates FlowCam data by date, TPU, and reactor

with flowcam_staging as (
    select * from {{ ref('stg_flowcam__records') }}
),

daily_aggregations as (
    select
        measurement_date,
        tpu_id,
        reactor_id,
        
        -- Aggregated metrics
        avg(algae_density) as avg_density,
        min(algae_density) as min_density,
        max(algae_density) as max_density,
        stddev(algae_density) as std_density,
        count(*) as measurement_count,
        
        -- Additional metrics
        max(algae_density) - min(algae_density) as density_range,
        case 
            when stddev(algae_density) > 0 
            then (max(algae_density) - min(algae_density)) / stddev(algae_density)
            else 0
        end as density_coefficient_variation,
        
        -- Quality indicators
        case 
            when count(*) >= 3 then 'high_confidence'
            when count(*) >= 2 then 'medium_confidence'
            else 'low_confidence'
        end as data_quality,
        
        -- Metadata
        min(loaded_at) as first_measurement,
        max(loaded_at) as last_measurement,
        current_timestamp as processed_at
        
    from flowcam_staging
    group by measurement_date, tpu_id, reactor_id
)

select * from daily_aggregations
