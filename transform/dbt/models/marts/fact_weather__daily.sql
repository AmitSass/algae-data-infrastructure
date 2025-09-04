-- Fact table for daily weather data
-- Business-ready analytics table

select
    weather_date,
    temperature_celsius,
    humidity_percentage,
    solar_radiation,
    -- Add calculated fields
    case 
        when temperature_celsius > 25 then 'Hot'
        when temperature_celsius < 15 then 'Cold'
        else 'Moderate'
    end as temperature_category,
    case 
        when solar_radiation > 800 then 'High'
        when solar_radiation > 400 then 'Medium'
        else 'Low'
    end as solar_category,
    current_timestamp as processed_at
from {{ ref('stg_weather__daily') }}
