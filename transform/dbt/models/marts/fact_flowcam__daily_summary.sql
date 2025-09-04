-- Fact table for daily FlowCam summaries
-- Business-ready analytics table

select
    measurement_date,
    tpu_id,
    reactor_id,
    avg_algae_density,
    min_algae_density,
    max_algae_density,
    measurement_count,
    -- Add calculated fields
    case 
        when avg_algae_density > 1.5 then 'High'
        when avg_algae_density > 1.0 then 'Medium'
        else 'Low'
    end as density_category,
    current_timestamp as processed_at
from {{ ref('int_flowcam__daily_summary') }}
