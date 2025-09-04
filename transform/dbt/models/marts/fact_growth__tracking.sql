{{ config(materialized='table', tags=['marts','fact','growth']) }}

select
  measurement_date,
  tpu_id,
  daily_growth_rate,
  biomass_density
from {{ ref('stg_growth__tracking') }}