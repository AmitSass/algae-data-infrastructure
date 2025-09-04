-- Dimension table for TPU (Tank Processing Unit) information
-- This model provides reference data for TPUs

with tpu_data as (
    select distinct tpu_id from {{ ref('stg_flowcam__records') }}
),

tpu_dimension as (
    select
        tpu_id,
        
        -- TPU attributes
        'TPU-' || lpad(tpu_id::text, 2, '0') as tpu_name,
        case 
            when tpu_id = 1 then 'North Wing'
            when tpu_id = 2 then 'South Wing'
            when tpu_id = 3 then 'East Wing'
            else 'Unknown Location'
        end as tpu_location,
        
        case 
            when tpu_id = 1 then 1000
            when tpu_id = 2 then 1200
            when tpu_id = 3 then 800
            else 500
        end as tpu_capacity_liters,
        
        case 
            when tpu_id = 1 then 'High Efficiency'
            when tpu_id = 2 then 'Standard'
            when tpu_id = 3 then 'Compact'
            else 'Basic'
        end as tpu_type,
        
        -- Status and metadata
        'Active' as status,
        current_timestamp as created_at,
        current_timestamp as updated_at
        
    from tpu_data
)

select * from tpu_dimension
