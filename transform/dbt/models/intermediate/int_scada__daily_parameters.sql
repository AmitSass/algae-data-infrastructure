-- Intermediate model for daily SCADA parameter summaries
-- Aggregates SCADA data by date, TPU, and parameter

select
    date(measurement_timestamp) as measurement_date,
    tpu_id,
    parameter_name,
    avg(parameter_value) as avg_value,
    min(parameter_value) as min_value,
    max(parameter_value) as max_value,
    count(*) as measurement_count,
    current_timestamp as processed_at
from {{ ref('stg_scada__raw') }}
group by date(measurement_timestamp), tpu_id, parameter_name
