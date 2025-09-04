-- Intermediate model for daily FlowCam summaries
-- Aggregates FlowCam data by date, TPU, and reactor

select
    measurement_date,
    tpu_id,
    reactor_id,
    avg(algae_density) as avg_algae_density,
    min(algae_density) as min_algae_density,
    max(algae_density) as max_algae_density,
    count(*) as measurement_count,
    current_timestamp as processed_at
from {{ ref('stg_flowcam__raw') }}
group by measurement_date, tpu_id, reactor_id
