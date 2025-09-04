{{ config(materialized='view', tags=['intermediate','flowcam']) }}

select
  a.measurement_date,
  a.tpu_id,
  a.reactor_id,
  a.avg_algae_density,
  case
    when a.avg_algae_density >= 1.5 then 'High'
    when a.avg_algae_density >= 0.8 then 'Medium'
    else 'Low'
  end as density_category
from {{ ref('int_flowcam__daily_agg') }} a