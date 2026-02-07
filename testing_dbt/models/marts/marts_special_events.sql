select
  borough,
  count(*) as total_incidents_during_special_events
from {{ ref('int_enrichment') }}
where special_event_indicator = 'Y'
group by borough
order by total_incidents_during_special_events desc
