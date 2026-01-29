select 
borough,
TRY_CAST(day_of_week_of_incident as VARCHAR) as day_of_week_of_incident,
count(*) as total_incidents

FROM {{ ref('int_enrichment') }}
group by borough,
day_of_week_of_incident
order by total_incidents, borough desc 
