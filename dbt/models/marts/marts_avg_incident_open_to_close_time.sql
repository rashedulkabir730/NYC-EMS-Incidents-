select avg(incident_datetime - incident_close_datetime) as avg_incident_open_to_close_time

from {{ ref('int_enrichment') }}