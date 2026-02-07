select
  borough,
  response_category,
  count(*) as total_incidents
from {{ ref('int_enrichment') }}
group by borough, response_category
order by total_incidents desc
