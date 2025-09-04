-- Staging model for harvest records
-- Standardizes harvest documentation

select
    harvest_date::date as harvest_date,
    tpu::integer as tpu_id,
    volume_harvested::float as volume_harvested_liters,
    quality_grade::varchar as quality_grade,
    current_timestamp as processed_at
from {{ source('raw_data', 'harvest_data') }}
