WITH flights_union AS (
    SELECT
        origin AS airport_code,
        dest AS connection_code,
        cancelled,
        diverted
    FROM {{ ref('prep_flights') }}

    UNION ALL

    SELECT
        dest AS airport_code,
        origin AS connection_code,
        cancelled,
        diverted
    FROM {{ ref('prep_flights') }}
),

agg AS (
    SELECT
        airport_code,
        COUNT(DISTINCT connection_code) AS unique_connections,
        COUNT(*) AS flight_count,
        SUM(cancelled) AS flight_cancelled,
        SUM(diverted) AS flight_diverted
    FROM flights_union
    GROUP BY airport_code
),

join_table AS (
    SELECT
        airport_code,
        unique_connections,
        flight_count,
        flight_cancelled,
        flight_diverted,
        a.city,
        a.country,
        a.name AS airport_name,
        min_temp_c,
        max_temp_c,
        precipitation_mm,
        max_snow_mm,
        avg_wind_direction,
        avg_wind_speed_kmh,
        wind_peakgust_kmh,
        make_date(date_year::int, date_month::int, date_day::int) AS date_daily
    FROM agg
    JOIN {{ ref('prep_airports') }} a
        ON agg.airport_code = a.faa
    JOIN {{ ref('prep_weather_daily') }} wd
        USING (airport_code)
)

SELECT
    airport_code,
    unique_connections,
    flight_count,
    flight_cancelled,
    flight_diverted,
    city,
    country,
    airport_name,
    min_temp_c,
    max_temp_c,
    precipitation_mm,
    max_snow_mm,
    avg_wind_direction,
    avg_wind_speed_kmh,
    wind_peakgust_kmh,
    date_daily
FROM join_table
