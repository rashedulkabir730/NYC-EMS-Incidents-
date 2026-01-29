WITH monthly AS (
    SELECT
        month_of_incident,
        COUNT(cad_incident_id) AS total_incidents
    FROM {{ ref('int_enrichment') }}
    GROUP BY month_of_incident
)

SELECT
    month_of_incident,
    total_incidents,
    total_incidents
        - LAG(total_incidents) OVER (ORDER BY month_of_incident) AS mom_change,
    (total_incidents
        - LAG(total_incidents) OVER (ORDER BY month_of_incident))
        / NULLIF(LAG(total_incidents) OVER (ORDER BY month_of_incident), 0) AS mom_pct_change
FROM monthly
