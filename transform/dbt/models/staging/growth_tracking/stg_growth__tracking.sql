-- Staging model for growth tracking data
-- Standardizes manual growth measurements

select
    date::date as measurement_date,
    tpu::integer as tpu_id,
    growth_rate::float as daily_growth_rate,
    biomass_density::float as biomass_density,
    current_timestamp as processed_at
from {{ source('raw_data', 'growth_tracking_data') }}
