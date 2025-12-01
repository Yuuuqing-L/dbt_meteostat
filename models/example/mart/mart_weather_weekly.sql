SELECT
    DATE_TRUNC('week', date) AS week_start,
    round(avg(avg_temp_c),2) AS avg_temp,
    min(min_temp_c) AS avg_min_temp,
    max(max_temp_c) AS avg_max_temp,
    SUM(precipitation_mm) AS total_precip_mm,
    max(max_snow_mm) AS max_snow_mm,
    AVG(avg_wind_speed_kmh) AS avg_wind_speed_kmh,
    avg(avg_pressure_hpa) AS avg_pressure_hpa
FROM  {{ ref('prep_weather_daily') }}
GROUP BY week_start
ORDER BY week_start
