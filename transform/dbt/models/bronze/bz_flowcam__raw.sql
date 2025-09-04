-- Bronze layer: Raw FlowCam data ingestion
-- This model loads raw FlowCam data from the source system

{{ config(
    materialized='table',
    schema='bronze'
) }}

select
    -- Raw data columns
    date,
    tpu,
    reactor,
    algae_density,
    
    -- Metadata
    current_timestamp as loaded_at,
    'flowcam_sample' as source_system,
    'bronze' as data_layer
    
from {{ source('raw', 'flowcam_raw') }}
