{{ config(materialized='table', tags=['marts','dim']) }}

with t as (
  select distinct tpu_id from {{ ref('stg_flowcam__records') }}
  union
  select distinct tpu_id from {{ ref('stg_scada__citect') }}
  union
  select distinct tpu_id from {{ ref('stg_growth__tracking') }}
  union
  select distinct tpu_id from {{ ref('stg_harvest__records') }}
)
select
  tpu_id,
  -- Demo attributes (placeholders). In prod you may join a real reference table.
  concat('TPU-', tpu_id) as tpu_name
from t