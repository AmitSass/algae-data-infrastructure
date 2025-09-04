{{ config(materialized='table', tags=['marts','fact','flowcam']) }}

select
  s.measurement_date,
  s.tpu_id,
  s.reactor_id,
  s.avg_algae_density,
  s.density_category
from {{ ref('int_flowcam__daily_summary') }} s