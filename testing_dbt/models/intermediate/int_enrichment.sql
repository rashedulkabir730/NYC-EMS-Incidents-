with extraction as (
select
    *,
    EXTRACT(DOW FROM incident_datetime) as day_of_week_of_incident,
    EXTRACT(MONTH from incident_datetime) as month_of_incident, 
    EXTRACT(YEAR FROM incident_datetime) as year_of_incident,
    EXTRACT(HOUR from incident_datetime) as hour_of_incident,

    CASE 
        WHEN incident_response_seconds_qy < 300 THEN 'Fast'
        WHEN incident_response_seconds_qy < 600 THEN 'Moderate'
        ELSE 'Slow'
    END as response_category,

FROM {{ ref('stg_incident') }}

),
incident_code_enrichment as (
select 
*,id_codes.Description

from extraction  e
LEFT JOIN {{ ref('EMS_incident_dispatch_data_description - Incident Dispositions') }} as id_codes
on e.incident_disposition_code=id_codes."INCIDENT DISPOSITION CODE"
),

inital_call_type_enrichment as (
select 
*,  
call_type_desc."CALL TYPE DESCRIPTION" as inital_call_type_desc
from incident_code_enrichment ide 
left join  {{ ref('EMS_incident_dispatch_data_description - Call Type Descriptions') }} as call_type_desc
on ide.initial_call_type=call_type_desc."CALL TYPE DESCRIPTION"
),

final_call_type_enrichment as(
select * , call_type_desc."CALL TYPE DESCRIPTION" as final_call_type_desc
from inital_call_type_enrichment icte
left join  {{ ref('EMS_incident_dispatch_data_description - Call Type Descriptions') }} as call_type_desc
on icte.final_call_type=call_type_desc."CALL TYPE DESCRIPTION"
)

select * from final_call_type_enrichment

