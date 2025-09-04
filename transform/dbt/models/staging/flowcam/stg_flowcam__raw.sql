{{ config(materialized='view', tags=['staging','flowcam']) }}
-- Prod note: when on Redshift, you can run COPY in a pre_hook to load from S3 (Parquet) into a raw table.

select
  cast(date as date)              as measurement_date,
  cast(tpu as integer)            as tpu_id,
  cast(reactor as integer)        as reactor_id,
  cast(algae_density as double precision) as algae_density
from {{ source('raw_data','flowcam_raw_data') }}
where date is not null and tpu is not null and reactor is not null