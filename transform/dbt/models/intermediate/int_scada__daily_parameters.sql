{{ config(materialized='view', tags=['intermediate','scada']) }}

select
  measurement_date,
  tpu_id,
  parameter_name,
  avg(parameter_value) as avg_value
from {{ ref('stg_scada__citect') }}
group by 1,2,3