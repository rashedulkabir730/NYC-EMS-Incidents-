select
  borough,
  ROUND(AVG(dispatch_response_seconds_qy), 2) as avg_dispatch_response_seconds,
  ROUND(AVG(incident_response_seconds_qy), 2) as avg_incident_response_seconds

from {{ ref('int_enrichment') }}
where
  valid_incident_rspns_time_indc = 'Y' and valid_dispatch_rspns_time_indc = 'Y'
group by borough
order by avg_dispatch_response_seconds asc, avg_incident_response_seconds desc
