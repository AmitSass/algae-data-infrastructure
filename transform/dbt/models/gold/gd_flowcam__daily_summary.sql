-- Gold layer: Daily FlowCam summary for analytics
-- This model provides the final aggregated view for business intelligence

{{ config(
    materialized='table',
    schema='gold'
) }}

with standardized_data as (
    select * from {{ ref('sl_flowcam__standardized') }}
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
        
        -- Quality metrics
        sum(case when is_outlier then 1 else 0 end) as outlier_count,
        sum(case when is_null then 1 else 0 end) as null_count,
        
        -- Calculated fields
        case 
            when avg(algae_density) < 0.5 then 'Very Low'
            when avg(algae_density) < 1.0 then 'Low'
            when avg(algae_density) < 1.5 then 'Medium'
            when avg(algae_density) < 2.0 then 'High'
            else 'Very High'
        end as density_category,
        
        case 
            when avg(algae_density) > 1.5 then 'Optimal'
            when avg(algae_density) > 1.0 then 'Good'
            when avg(algae_density) > 0.5 then 'Acceptable'
            else 'Below Target'
        end as performance_status,
        
        -- Metadata
        min(processed_at) as first_processed,
        max(processed_at) as last_processed,
        current_timestamp as created_at
        
    from standardized_data
    group by measurement_date, tpu_id, reactor_id
)

select * from daily_aggregations
