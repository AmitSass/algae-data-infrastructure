-- Fact table for daily SCADA metrics
-- Business-ready analytics table

select
    measurement_date,
    tpu_id,
    parameter_name,
    avg_value,
    min_value,
    max_value,
    measurement_count,
    -- Add calculated fields
    case 
        when parameter_name = 'temperature' and avg_value > 25 then 'Hot'
        when parameter_name = 'temperature' and avg_value < 15 then 'Cold'
        else 'Normal'
    end as temperature_category,
    current_timestamp as processed_at
from {{ ref('int_scada__daily_parameters') }}
