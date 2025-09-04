-- Dimension table for TPU (Tank Processing Unit) reference data
-- Provides standardized TPU information

select
    tpu_id,
    'TPU ' || tpu_id::varchar as tpu_name,
    case 
        when tpu_id <= 3 then 'Primary'
        when tpu_id <= 6 then 'Secondary'
        else 'Tertiary'
    end as tpu_category,
    case 
        when tpu_id in (1, 2, 3) then 'North Wing'
        when tpu_id in (4, 5, 6) then 'South Wing'
        else 'East Wing'
    end as location_wing,
    current_timestamp as processed_at
from (
    select distinct tpu_id
    from {{ ref('stg_flowcam__raw') }}
    union
    select distinct tpu_id
    from {{ ref('stg_scada__raw') }}
    union
    select distinct tpu_id
    from {{ ref('stg_growth__tracking') }}
    union
    select distinct tpu_id
    from {{ ref('stg_harvest__records') }}
) tpu_list