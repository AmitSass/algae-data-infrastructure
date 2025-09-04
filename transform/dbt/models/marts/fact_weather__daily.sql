{{ config(materialized='table', tags=['marts','fact','weather']) }}

select
  weather_date,
  temperature_celsius,
  humidity_percentage,
  solar_radiation
from {{ ref('stg_weather__daily') }}