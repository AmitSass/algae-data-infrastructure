{{ config(materialized='view', tags=['staging','flowcam']) }}
-- Standardize, de-duplicate, add simple features

with src as (
  select * from {{ ref('stg_flowcam__raw') }}
),
dedup as (
  select *
  from (
    select
      *,
      row_number() over (
        partition by measurement_date, tpu_id, reactor_id
        order by measurement_date desc
      ) as rn
    from src
  ) t
  where rn = 1
)
select
  measurement_date,
  tpu_id,
  reactor_id,
  algae_density,
  case
    when algae_density >= 1.5 then 'High'
    when algae_density >= 0.8 then 'Medium'
    else 'Low'
  end as density_category
from dedup