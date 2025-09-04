-- Staging model for FlowCam raw data
-- Applies basic transformations and type casting

select
    date::date as measurement_date,
    tpu::integer as tpu_id,
    reactor::integer as reactor_id,
    algae_density::float as algae_density,
    current_timestamp as processed_at
from {{ source('raw_data', 'flowcam_raw_data') }}
