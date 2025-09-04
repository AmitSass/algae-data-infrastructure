-- Fact table for growth tracking data
-- Business-ready analytics table

select
    measurement_date,
    tpu_id,
    daily_growth_rate,
    biomass_density,
    -- Add calculated fields
    case 
        when daily_growth_rate > 0.1 then 'High Growth'
        when daily_growth_rate > 0.05 then 'Medium Growth'
        else 'Low Growth'
    end as growth_category,
    current_timestamp as processed_at
from {{ ref('stg_growth__tracking') }}
