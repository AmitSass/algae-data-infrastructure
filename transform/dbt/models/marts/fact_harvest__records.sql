{{ config(materialized='table', tags=['marts','fact','harvest']) }}

select
  harvest_date,
  tpu_id,
  volume_harvested_liters,
  quality_grade
from {{ ref('stg_harvest__records') }}