-- Fact table for harvest records
-- Business-ready analytics table

select
    harvest_date,
    tpu_id,
    volume_harvested_liters,
    quality_grade,
    -- Add calculated fields
    case 
        when volume_harvested_liters > 1000 then 'Large Harvest'
        when volume_harvested_liters > 500 then 'Medium Harvest'
        else 'Small Harvest'
    end as harvest_size_category,
    current_timestamp as processed_at
from {{ ref('stg_harvest__records') }}
