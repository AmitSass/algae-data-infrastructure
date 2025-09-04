{{ config(materialized='view', tags=['staging','scada']) }}
-- Alias/normalized view over SCADA raw (kept for clarity of naming)

select
  date_trunc('day', measurement_timestamp) as measurement_date,
  tpu_id,
  parameter_name,
  parameter_value
from {{ ref('stg_scada__raw') }}