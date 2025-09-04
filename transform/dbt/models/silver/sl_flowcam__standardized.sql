-- Silver layer: Standardized FlowCam data
-- This model standardizes and cleans the raw FlowCam data

{{ config(
    materialized='table',
    schema='silver'
) }}

with raw_data as (
    select * from {{ ref('bz_flowcam__raw') }}
),

standardized as (
    select
        -- Primary key
        date::date as measurement_date,
        tpu::integer as tpu_id,
        reactor::integer as reactor_id,
        
        -- Standardized columns
        algae_density::decimal(10,3) as algae_density,
        
        -- Data quality flags
        case 
            when algae_density < 0 or algae_density > 3.0 then true
            else false
        end as is_outlier,
        
        case 
            when algae_density is null then true
            else false
        end as is_null,
        
        -- Metadata
        loaded_at,
        source_system,
        data_layer,
        current_timestamp as processed_at
        
    from raw_data
    where 
        date is not null
        and tpu is not null
        and reactor is not null
        and algae_density is not null
)

select * from standardized
