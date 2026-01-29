WITH src AS (
  SELECT * FROM {{ source('raw', 'raw_api_data') }}
),

type_fixes AS (
  SELECT
    TRIM(cad_incident_id) AS cad_incident_id,
    TRIM(initial_call_type) AS initial_call_type,
    initial_severity_level_code,
    TRIM(final_call_type) AS final_call_type,
    UPPER(TRIM(borough)) AS borough,
    TRIM(incident_dispatch_area) AS incident_dispatch_area,
    final_severity_level_code,
    valid_dispatch_rspns_time_indc,
    valid_incident_rspns_time_indc,

    held_indicator,
    incident_disposition_code,
    zipcode,
    reopen_indicator,
    special_event_indicator,
    standby_indicator,
    transfer_indicator,

    TRY_CAST(incident_datetime AS TIMESTAMP) AS incident_datetime,
    TRY_CAST(first_assignment_datetime AS TIMESTAMP) AS first_assignment_datetime,
    TRY_CAST(first_activation_datetime AS TIMESTAMP) AS first_activation_datetime,
    TRY_CAST(first_on_scene_datetime AS TIMESTAMP) AS first_on_scene_datetime,
    TRY_CAST(first_to_hosp_datetime AS TIMESTAMP) AS first_to_hosp_datetime,
    TRY_CAST(src.first_hosp_arrival_datetime AS TIMESTAMP) AS first_to_hosp_arrival_datetime,
    TRY_CAST(incident_close_datetime AS TIMESTAMP) AS incident_close_datetime,

    TRY_CAST(dispatch_response_seconds_qy AS INT) AS dispatch_response_seconds_qy,
    TRY_CAST(incident_travel_tm_seconds_qy AS INT) AS incident_travel_tm_seconds_qy,
    TRY_CAST(incident_response_seconds_qy AS INT) AS incident_response_seconds_qy,
    TRY_CAST(policeprecinct AS INT) AS policeprecinct,
    TRY_CAST(citycouncildistrict AS INT) AS citycouncildistrict,
    TRY_CAST(communitydistrict AS INT) AS communitydistrict,
    TRY_CAST(communityschooldistrict AS INT) AS communityschooldistrict,
    TRY_CAST(congressionaldistrict AS INT) AS congressionaldistrict
  FROM src
),

deduped AS (
  SELECT *
  FROM type_fixes
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY cad_incident_id
    ORDER BY
      incident_close_datetime DESC NULLS LAST,
      first_assignment_datetime DESC NULLS LAST,
      incident_datetime DESC NULLS LAST
  ) = 1
)

SELECT
  cad_incident_id,
  incident_datetime,
  first_assignment_datetime,
  first_activation_datetime,
  first_on_scene_datetime,
  first_to_hosp_datetime,
  first_to_hosp_arrival_datetime,
  incident_close_datetime,
  dispatch_response_seconds_qy,
  incident_response_seconds_qy,
  incident_travel_tm_seconds_qy,
  valid_dispatch_rspns_time_indc,
  valid_incident_rspns_time_indc,
  held_indicator,
  reopen_indicator,
  special_event_indicator,
  standby_indicator,
  transfer_indicator,
  initial_call_type,
  final_call_type,
  initial_severity_level_code,
  final_severity_level_code,
  incident_disposition_code,
  borough,
  incident_dispatch_area,
  zipcode,
  policeprecinct,
  citycouncildistrict,
  communitydistrict,
  communityschooldistrict,
  congressionaldistrict
FROM deduped
where borough <> 'UNKNOWN'
