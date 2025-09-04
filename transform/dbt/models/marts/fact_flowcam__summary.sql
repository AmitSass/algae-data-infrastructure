{{ config(materialized='table', tags=['marts','fact','flowcam']) }}
-- Facility-level daily summary (rollup)

with base as (
  select * from {{ ref('int_flowcam__daily_agg') }}
)
select
  measurement_date,
  tpu_id,
  avg(avg_algae_density) as avg_density_tpu
from base
group by 1,2