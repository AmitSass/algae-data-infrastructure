{{ config(materialized='view', tags=['staging','growth']) }}

select
  cast(date as date)              as measurement_date,
  cast(tpu as integer)            as tpu_id,
  cast(growth_rate as double precision)    as daily_growth_rate,
  cast(biomass_density as double precision) as biomass_density
from {{ source('raw_data','growth_tracking_data') }}
where date is not null and tpu is not null