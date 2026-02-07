select  
final_call_type_desc,
COUNT(*) as total_incidents

from {{ ref('int_enrichment') }}
group by final_call_type_desc
order by total_incidents desc
