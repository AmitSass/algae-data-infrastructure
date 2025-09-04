{{ config(materialized='view', tags=['staging','scada']) }}

select
  cast(datetime as timestamp)     as measurement_timestamp,
  cast(tpu as integer)            as tpu_id,
  cast(parameter_name as text)    as parameter_name,
  cast(value as double precision) as parameter_value
from {{ source('raw_data','scada_raw_data') }}
where datetime is not null and tpu is not null and parameter_name is not null