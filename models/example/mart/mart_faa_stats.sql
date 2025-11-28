WITH flights_union AS (
    -- departures
    SELECT
        origin AS airport_code,
        dest AS connection_code,
        cancelled,
        diverted,
        tail_number,
        airline
    FROM {{ ref('prep_flights') }}

    UNION ALL

    -- arrivals
    SELECT
        dest AS airport_code,
        origin AS connection_code,
        cancelled,
        diverted,
        tail_number,
        airline
    FROM {{ ref('prep_flights') }}
),
agg AS (
    SELECT
        airport_code,

        -- unique departure + arrival connections
        COUNT(DISTINCT connection_code) AS unique_connections,

        -- total flights (arrivals + departures)
        COUNT(*) AS flight_count,

        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_total,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS diverted_total,
        SUM(CASE WHEN cancelled = 0 THEN 1 ELSE 0 END) AS occurred_total,

        -- optional metrics
        COUNT(DISTINCT tail_number) AS unique_airplanes,
        COUNT(DISTINCT airline) AS unique_airlines
    FROM flights_union
    GROUP BY airport_code
)
SELECT
    a.airport_code,
    ap.name,
    ap.city,
    ap.country,
    ap.faa,
    a.unique_connections,
    a.flight_count,
    a.cancelled_total,
    a.diverted_total,
    a.occurred_total,
    a.unique_airplanes,
    a.unique_airlines

FROM agg a
LEFT JOIN {{ ref('prep_airports') }} ap
    ON a.airport_code = ap.faa
ORDER BY airport_code;
