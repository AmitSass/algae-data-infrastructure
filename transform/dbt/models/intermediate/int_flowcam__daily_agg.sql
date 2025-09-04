{{ config(materialized='ephemeral', tags=['intermediate','flowcam']) }}
-- Daily aggregation (used by multiple downstream models)

select
  measurement_date,
  tpu_id,
  reactor_id,
  avg(algae_density)       as avg_algae_density,
  min(algae_density)       as min_algae_density,
  max(algae_density)       as max_algae_density,
  count(*)                  as records_cnt
from {{ ref('stg_flowcam__records') }}
group by 1,2,3