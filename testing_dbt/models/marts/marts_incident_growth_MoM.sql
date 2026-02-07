with monthly as (
  select
    month_of_incident,
    COUNT(cad_incident_id) as total_incidents
  from {{ ref('int_enrichment') }}
  group by month_of_incident
)

select
  month_of_incident,
  total_incidents,
  total_incidents
  - LAG(total_incidents) over (order by month_of_incident) as mom_change,
  (
    total_incidents
    - LAG(total_incidents) over (order by month_of_incident)
  )
  / NULLIF(LAG(total_incidents) over (order by month_of_incident), 0)
    as mom_pct_change
from monthly
