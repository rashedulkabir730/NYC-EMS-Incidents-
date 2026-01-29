WITH borough_counts AS (
    SELECT
        borough,
        COUNT(*) AS total_incidents
    FROM {{ ref('int_enrichment') }}
    GROUP BY borough
)

SELECT
    borough,
    total_incidents,
    RANK() OVER (ORDER BY total_incidents DESC) AS rank
FROM borough_counts
