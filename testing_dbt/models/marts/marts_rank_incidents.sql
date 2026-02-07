with borough_counts as (
  select
    borough,
    COUNT(*) as total_incidents
  from {{ ref('int_enrichment') }}
  group by borough
)

select
  borough,
  total_incidents,
  RANK() over (order by total_incidents desc) as rank
from borough_counts
