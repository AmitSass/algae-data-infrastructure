-- Staging model for weather data
-- Standardizes weather measurements

select
    date::date as weather_date,
    temperature::float as temperature_celsius,
    humidity::float as humidity_percentage,
    solar_radiation::float as solar_radiation,
    current_timestamp as processed_at
from {{ source('raw_data', 'weather_data') }}
