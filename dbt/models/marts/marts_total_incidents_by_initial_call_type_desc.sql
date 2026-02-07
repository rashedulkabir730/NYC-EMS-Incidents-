select  
inital_call_type_desc,
COUNT(*) as total_incidents

from {{ ref('int_enrichment') }}
group by inital_call_type_desc
order by total_incidents desc