{{ config(materialized='table', tags=['marts','fact','scada']) }}

select
  measurement_date,
  tpu_id,
  parameter_name,
  avg_value
from {{ ref('int_scada__daily_parameters') }}