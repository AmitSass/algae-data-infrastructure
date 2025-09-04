-- Staging model for FlowCam records
-- This model standardizes and cleans raw FlowCam data

with source_data as (
    select * from {{ source('raw', 'flowcam_raw') }}
),

cleaned_data as (
    select
        -- Primary key
        date,
        tpu,
        reactor,
        
        -- Standardized columns
        date::date as measurement_date,
        tpu::integer as tpu_id,
        reactor::integer as reactor_id,
        algae_density::decimal(10,3) as algae_density,
        
        -- Metadata
        current_timestamp as loaded_at,
        'flowcam_sample' as source_file
        
    from source_data
    where 
        date is not null
        and tpu is not null
        and reactor is not null
        and algae_density is not null
        and algae_density >= 0
        and algae_density <= 3.0
)

select * from cleaned_data
