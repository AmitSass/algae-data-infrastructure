{{ config(materialized='view', tags=['staging','weather']) }}

select
  cast(date as date)              as weather_date,
  cast(temperature as double precision)       as temperature_celsius,
  cast(humidity as double precision)          as humidity_percentage,
  cast(solar_radiation as double precision)   as solar_radiation
from {{ source('raw_data','weather_data') }}
where date is not null