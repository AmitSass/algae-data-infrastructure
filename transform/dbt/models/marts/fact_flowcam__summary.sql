-- Fact table for FlowCam summary data
-- This model provides the final aggregated view for analytics

with daily_agg as (
    select * from {{ ref('int_flowcam__daily_agg') }}
),

tpu_dim as (
    select * from {{ ref('dim_tpu') }}
),

final_summary as (
    select
        -- Primary key
        measurement_date,
        daily_agg.tpu_id,
        daily_agg.reactor_id,
        
        -- Dimension attributes
        tpu_dim.tpu_name,
        tpu_dim.tpu_location,
        tpu_dim.tpu_capacity,
        
        -- Fact metrics
        avg_density,
        min_density,
        max_density,
        std_density,
        measurement_count,
        density_range,
        density_coefficient_variation,
        data_quality,
        
        -- Calculated fields
        case 
            when avg_density < 0.5 then 'Very Low'
            when avg_density < 1.0 then 'Low'
            when avg_density < 1.5 then 'Medium'
            when avg_density < 2.0 then 'High'
            else 'Very High'
        end as density_category,
        
        case 
            when avg_density > 1.5 then 'Optimal'
            when avg_density > 1.0 then 'Good'
            when avg_density > 0.5 then 'Acceptable'
            else 'Below Target'
        end as performance_status,
        
        -- Metadata
        first_measurement,
        last_measurement,
        processed_at
        
    from daily_agg
    left join tpu_dim on daily_agg.tpu_id = tpu_dim.tpu_id
)

select * from final_summary
