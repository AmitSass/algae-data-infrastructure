{{ config(materialized='view', tags=['staging','harvest']) }}

select
  cast(harvest_date as date)      as harvest_date,
  cast(tpu as integer)            as tpu_id,
  cast(volume_harvested as double precision) as volume_harvested_liters,
  cast(quality_grade as text)     as quality_grade
from {{ source('raw_data','harvest_data') }}
where harvest_date is not null and tpu is not null