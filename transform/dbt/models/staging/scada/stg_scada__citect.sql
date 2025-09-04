-- Staging model for SCADA Citect data
-- Standardizes SCADA parameter measurements

select
    datetime::timestamp as measurement_timestamp,
    tpu::integer as tpu_id,
    parameter_name::varchar as parameter_name,
    value::float as parameter_value,
    current_timestamp as processed_at
from {{ source('raw_data', 'scada_citect_data') }}
